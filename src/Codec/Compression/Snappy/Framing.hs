{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Codec.Compression.Snappy.Framing
-- Copyright   : (c) 2013 Kim Altintop <kim.altintop@gmail.com>
-- License     : This Source Code Form is subject to the terms of
--               the Mozilla Public License, v. 2.0.
--               A copy of the MPL can be found in the LICENSE file or
--               you can obtain it at http://mozilla.org/MPL/2.0/.
-- Maintainer  : Kim Altintop <kim.altintop@gmail.com>
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--

module Codec.Compression.Snappy.Framing
    ( -- * Encoding and Decoding
      encode
    , encode'
    , decode
    , decode'
    , decodeVerify
    , decodeVerify'
    , decodeM
    , decodeVerifyM

    -- * Utility functions
    , checksum
    , streamIdentifier
    , verify
    ) where

import Control.Applicative
import Data.ByteString     (ByteString)
import Data.Binary         (Binary(..))
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.Digest.CRC32C
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

maxUncompressed :: Int
maxUncompressed = 65536

minCompressible :: Int
minCompressible = 18

instance Binary Chunk where
    put StreamIdentifier       = mapM_ put streamStart
    put (Compressed   chk dat) = putWord8 0x00 >> putData chk dat
    put (Uncompressed chk dat) = putWord8 0x01 >> putData chk dat
    put (Skippable    x)       = put x
    put (Unskippable  x)       = put x

    get = do
        chunktype <- getWord8
        case chunktype of
            0xff -> skip (length streamStart - 1) >> return StreamIdentifier
            0x00 -> return . uncurry Compressed   =<< getData
            0x01 -> return . uncurry Uncompressed =<< getData
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

-- | Verify a 'Chunk'
--
-- Returns 'Nothing' if the input is an 'Unskippable' chunk, or the checksum
-- verification fails (if the input is a 'Compressed' or 'Uncompressed' chunk).
-- Otherwise, the input 'Chunk' is returned in a 'Just'. Note that 'Compressed'
-- chunks are decompressed into 'Uncompressed' chunks on the fly.
verify :: Chunk -> Maybe Chunk
verify u@(Uncompressed chk d) = if chk == checksum d then Just u else Nothing
verify (Compressed chk d)     = if ok then Just (Uncompressed chk d') else Nothing
  where
    d' = Snappy.decompress d
    ok = chk == checksum d'
verify (Unskippable _)        = Nothing
verify c                      = Just c

-- | Yield a stream identifier (start-of-stream marker)
streamIdentifier :: BL.ByteString
streamIdentifier = Binary.encode StreamIdentifier

-- | Encode a lazy 'BL.ByteString' into a 'Chunk'
--
-- If the input is longer than 'minCompressible' bytes, the resulting chunk is
-- 'Compressed' otherwise 'Uncompressed'. If the input size exceeds
-- 'maxUncompressed' bytes, the leftover input is returned in a 'Just'.
encode :: BL.ByteString -> (Chunk, Maybe BL.ByteString)
encode = go . split
  where
    go (x,xs) = (chunk $ BL.toStrict x, leftover' xs)

    chunk c
      | shouldCompress c = Compressed   (checksum c) (Snappy.compress c)
      | otherwise        = Uncompressed (checksum c) c

    split = BL.splitAt (fromIntegral maxUncompressed)

    leftover' x
      | BL.null x = Nothing
      | otherwise = Just x

-- | Encode a strict 'ByteString' into a 'Chunk'
--
-- If the input is longer than 'minCompressible' bytes, the resulting chunk is
-- 'Compressed' otherwise 'Uncompressed'. If the input size exceeds
-- 'maxUncompressed' bytes, the leftover input is returned in a 'Just'.
encode' :: ByteString -> (Chunk, Maybe ByteString)
encode' = go . split
  where
    go (x,xs) = (chunk x, leftover xs)

    chunk c
      | shouldCompress c = Compressed   (checksum c) (Snappy.compress c)
      | otherwise        = Uncompressed (checksum c) c

    split = B.splitAt maxUncompressed


type Error = (ByteOffset, String)

-- | Decode a lazy 'BL.ByteString' into a 'Chunk'
decode :: BL.ByteString -> (Either Error Chunk, Maybe ByteString)
decode = dec . feed

-- | Decode a lazy 'BL.ByteString' into a 'Chunk' and 'verify' the result
decodeVerify :: BL.ByteString -> (Either Error Chunk, Maybe ByteString)
decodeVerify = decV . feed

-- | Decode a strict 'ByteString' into a 'Chunk'
decode' :: ByteString -> (Either Error Chunk, Maybe ByteString)
decode' = dec . feed'

-- | Decode a strict 'ByteString' into a 'Chunk' and 'verify' the result
decodeVerify' :: ByteString -> (Either Error Chunk, Maybe ByteString)
decodeVerify' = decV . feed'

-- | Decode drawing input from the given monadic action as needed
decodeM :: Monad m
        => m (Maybe ByteString)
        -- ^ And action that will be run to provide input. If it returns
        -- 'Nothing' it is assumed no more input is available.
        -> m (Either Error Chunk, Maybe ByteString)
        -- ^ Either a parse error or a 'Chunk', along with leftovers if any.
decodeM pull = go (runGetIncremental (get :: Get Chunk))
  where
    go (Partial k)  = go . k =<< pull
    go (Fail r n m) = return (Left (n, m), leftover r)
    go (Done r _ c) = return (Right c,     leftover r)

-- | Like 'decodeM', but 'verify' the result
decodeVerifyM :: Monad m
              => m (Maybe ByteString)
              -> m (Either Error Chunk, Maybe ByteString)
decodeVerifyM pull = go (runGetIncremental (get :: Get Chunk))
  where
    go (Partial k)  = go . k =<< pull
    go (Fail r n m) = return (Left (n, m), leftover r)
    go (Done r n c) = case verify c of
                          Just c' -> return (Right c', leftover r)
                          Nothing -> go (Fail r n "verification failure")

--
-- Internal
--

shouldCompress :: ByteString -> Bool
shouldCompress x = B.length x >= minCompressible
{-# INLINEABLE shouldCompress #-}

feed :: BL.ByteString -> Decoder Chunk
feed = pushChunks $ runGetIncremental get
{-# INLINEABLE feed #-}

feed' :: ByteString -> Decoder Chunk
feed' = pushChunk $ runGetIncremental get
{-# INLINEABLE feed' #-}

dec :: Decoder Chunk -> (Either Error Chunk, Maybe ByteString)
dec (Partial k)  = dec (k Nothing)
dec (Fail r n m) = (Left (n, m), leftover r)
dec (Done r _ c) = (Right c,     leftover r)
{-# INLINEABLE dec #-}

decV :: Decoder Chunk -> (Either Error Chunk, Maybe ByteString)
decV (Partial k)  = decV (k Nothing)
decV (Fail r n m) = (Left (n, m), leftover r)
decV (Done r n c) = case verify c of
                        Just c' -> (Right c', leftover r)
                        Nothing -> decV (Fail r n "verification failure")
{-# INLINEABLE decV #-}

leftover :: ByteString -> Maybe ByteString
leftover x
  | B.null x  = Nothing
  | otherwise = Just x
{-# INLINEABLE leftover #-}
