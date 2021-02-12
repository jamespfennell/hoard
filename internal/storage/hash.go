package storage

import (
	"crypto/sha256"
	"encoding/base32"
)

type Hash string

const encodeStd = "abcdefghijklmnopqrstuvwxyz234567"

func CalculateHash(b []byte) Hash {
	h := sha256.New()
	// hash.Hash#Write never returns an error
	_, _ = h.Write(b)
	return Hash(base32.NewEncoding(encodeStd).EncodeToString(h.Sum(nil))[:12])
}

func ExampleHash() Hash {
	return "aaaaaaaaaaaa"
}
