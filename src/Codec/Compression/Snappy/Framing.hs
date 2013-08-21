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
    , encodeChunks
    , fromChunks
    , toChunks

    -- * Utility functions
    , checksum
    , verify
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
fromChunks :: [Chunk] -> BL.ByteString
fromChunks = BL.concat . map unchunk
  where
    unchunk (Compressed   _ d) = BL.fromStrict $ Snappy.decompress d
    unchunk (Uncompressed _ d) = BL.fromStrict d
    unchunk _                  = BL.empty

-- | 'toChunks' the input and concatenate the result, prepended by a stream
-- identifier.
encode :: BL.ByteString -> BL.ByteString
encode bs = Binary.encode StreamIdentifier <> encodeChunks bs

-- | Like 'encode', but don't prepend the stream identifier. Useful for
-- incremental encoding.
encodeChunks :: BL.ByteString -> BL.ByteString
encodeChunks bs = BL.concat $ map Binary.encode (toChunks bs)

-- | Decode a previously 'encode'd stream
decode :: BL.ByteString -> BL.ByteString
decode = fromChunks . decodeChunks

-- | Decode a previously 'encode'd stream into 'Chunk's
--
-- Decoding stops in case of a decoding error, a checksum verification failure,
-- or an 'Unskippable' chunk header. Note that compressed 'Chunk's are
-- decompressed on the fly.
decodeChunks :: BL.ByteString -> [Chunk]
decodeChunks = go [] . feed
  where
    go acc (Done rest _ chunk) =
        let acc' = maybe acc (:acc) (verify chunk)
         in if B.null rest then acc' else go acc' (feed $ BL.fromChunks [rest])

    go acc (Fail _ _ _) = acc
    go acc (Partial k)  = go acc (k Nothing)

    feed = pushChunks (runGetIncremental (get :: Get Chunk))

-- | Verify a 'Chunk'
--
-- Returns 'Nothing' if the input is an 'Unskippable' chunk, or the checksum
-- verification fails (if the input is a 'Compressed' or 'Uncompressed' chunk).
-- Otherwise, the input 'Chunk' is returned in a 'Just'. Note that 'Compressed'
-- chunks are deompressed into 'Uncompressed' chunks on the fly.
verify :: Chunk -> Maybe Chunk
verify u@(Uncompressed chk d) = if chk == checksum d then Just u else Nothing
verify (Compressed chk d)     = if ok then Just (Uncompressed chk d') else Nothing
  where
    d' = Snappy.decompress d
    ok = chk == checksum d'
verify (Unskippable _)        = Nothing
verify c                      = Just c
