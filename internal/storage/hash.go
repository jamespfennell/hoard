package storage

import (
	"crypto/sha256"
	"encoding/base32"
)

type Hash string

const encodeStd = "abcdefghijklmnopqrstuvwxyz234567"

func CalculateHash(b []byte) (Hash, error) {
	h := sha256.New()
	_, err := h.Write(b)
	if err != nil {
		return "", err
	}
	return Hash(base32.NewEncoding(encodeStd).EncodeToString(h.Sum(nil))[:12]), nil
}
