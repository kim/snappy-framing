{-# LANGUAGE OverloadedStrings #-}

module Codec.Compression.Snappy.Framing
    ( -- * Constants
      streamStart
    , maxUncompressed
    , minCompressible

    -- * Encoding and Decoding
    , decode
    , decodeChunks
    , encode
    , fromChunks
    , toChunks

    -- * Utility functions
    , checksum
    ) where

import Control.Applicative
import Data.ByteString          (ByteString)
import Data.Binary              (Binary(..))
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.Digest.CRC32C
import Data.Int
import Data.Monoid
import Data.Word

import qualified Data.Binary              as Binary
import qualified Data.ByteString          as B
import qualified Data.ByteString.Lazy     as BL
import qualified Codec.Compression.Snappy as Snappy


type Checksum = Word32

data Chunk = StreamIdentifier
           | Compressed   !Checksum !ByteString
           | Uncompressed !Checksum !ByteString
           | Skippable    !Word8
           | Unskippable  !Word8
    deriving (Eq, Show)


streamStart :: [Word8]
streamStart = [0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59]

maxUncompressed :: Int64
maxUncompressed = 65536

minCompressible :: Int
minCompressible = 18

instance Binary Chunk where
    put StreamIdentifier = mapM_ put streamStart
    put (Compressed chk dat) = do
        putWord8 0x00
        putData chk dat
    put (Uncompressed chk dat) = do
        putWord8 0x01
        putData chk dat
    put (Skippable x)   = put x
    put (Unskippable x) = put x

    get = do
        chunktype <- getWord8
        case chunktype of
            0xff -> do
                _ <- skip (length streamStart - 1)
                return StreamIdentifier
            0x00 -> do
                (chk,dat) <- getData
                return $ Compressed chk dat
            0x01 -> do
                (chk,dat) <- getData
                return $ Uncompressed chk dat
            x | x >= 0x02 && x <= 0x7f -> return $ Unskippable x
              | x >= 0x80 && x <= 0xfe -> return $ Skippable x
              | otherwise -> error "junk chunk type"

getData :: Get (Checksum, ByteString)
getData = do
    len <- getWord24le
    chk <- getWord32le
    dat <- getByteString . fromIntegral $ len - 4
    return (chk, dat)

putData :: Checksum -> ByteString -> Put
putData chk dat = do
    putWord24le (B.length dat + 4)
    putWord32le chk
    putByteString dat

getWord24le :: Get Word32
getWord24le = do
    (a,b,c) <- (,,) <$> getWord8 <*> getWord8 <*> getWord8
    return $  (fromIntegral a :: Word32)
          .|. (fromIntegral b :: Word32) `shiftL` 8
          .|. (fromIntegral c :: Word32) `shiftL` 16

putWord24le :: Int -> Put
putWord24le x = mapM_ putWord8 bytes
  where
    bytes = [ fromIntegral ((fromIntegral x :: Word32) `shiftR` 0)  :: Word8
            , fromIntegral ((fromIntegral x :: Word32) `shiftR` 8)  :: Word8
            , fromIntegral ((fromIntegral x :: Word32) `shiftR` 16) :: Word8
            ]

-- | Compute a masked CRC32C checksum of the input
checksum :: ByteString -> Checksum
checksum a =
    let chksum = crc32c a
        masked = ((chksum `shiftR` 15) .|. (chksum `shiftL` 17)) + 0xa282ead8
     in masked

-- | Split the input into a stream of 'Chunk's
--
-- The input is split into chunks of max 'maxUncompressed' bytes. If a chunk is
-- larger than 'minCompressible' bytes it is compressed, otherwise an
-- uncompressed 'Chunk' is created.
toChunks :: BL.ByteString -> [Chunk]
toChunks = go . split
  where
    go (x,xs) = chunk (BL.toStrict x) : if BL.null xs then [] else go (split xs)

    chunk c | min_comp c = Compressed (checksum c) (Snappy.compress c)
            | otherwise  = Uncompressed (checksum c) c

    split = BL.splitAt maxUncompressed

    min_comp x = B.length x >= minCompressible

-- | Concatenate a list of 'Chunk's into a lazy 'ByteString'.
--
-- Each 'Chunk''s checksum is verified against the actual 'checksum' of it's
-- data. A checksum verification failure is an error.
fromChunks :: [Chunk] -> BL.ByteString
fromChunks = foldl (\ bs c -> bs <> unchunk c) BL.empty
  where
    unchunk (Compressed   chk d) = BL.fromStrict $ verify chk (Snappy.decompress d)
    unchunk (Uncompressed chk d) = BL.fromStrict $ verify chk d
    unchunk _ = BL.empty

    verify chk bs =
        let chk' = checksum bs
         in if chk /= chk'
                then error $ "crc mismatch: " ++ show chk ++ " /= " ++ show chk'
                else bs

-- | 'toChunks' the input and concatenate the result, prepended by a stream
-- identifier.
encode :: BL.ByteString -> BL.ByteString
encode bs = BL.concat $ map Binary.encode (StreamIdentifier : toChunks bs)

-- | Decode a previously 'encode'd stream
decode :: BL.ByteString -> BL.ByteString
decode = fromChunks . decodeChunks

-- | Decode a previously 'encode'd stream into 'Chunk's
decodeChunks :: BL.ByteString -> [Chunk]
decodeChunks bs =
    let res = pushChunks (runGetIncremental (get :: Get Chunk)) bs
     in case res of
            Done r _ val   -> val : if B.null r then [] else decodeChunks (BL.fromChunks [r])
            Fail _ pos err -> error $ "fack: " ++ show pos ++ ": " ++ err
            Partial _      -> error "wtfpartial"
