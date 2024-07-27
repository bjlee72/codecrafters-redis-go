package storage

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"

	lzf "github.com/zhuyie/golzf"
)

// EmptyRDBString returns the empty RDB string representation.
func EmptyRDBString() (string, error) {
	var (
		b64 = `UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==`
	)

	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", fmt.Errorf("base64.StdEncoding.DecodeString: %w", err)
	}

	return string(decoded), nil
}

// ReadRDBToCache reads the contents of the RDB file to the given cache.
func ReadRDBToCache(dir, filename string, cache *Cache) error {
	path := fmt.Sprintf("%s/%s", dir, filename)

	// empty the cache completely.
	cache.Reset()

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("file open failed: %w", err)
	}

	// for now, we ignore the version number.
	_, err = readHeader(f)
	if err != nil {
		return fmt.Errorf("couldn't read header: %w", err)
	}

	// now we read op code. based on the value, we decide what to do.
	for {
		opcode := make([]byte, 1)
		if err := read(f, opcode); err != nil {
			return fmt.Errorf("couldn't read opcode: %w", err)
		}

		switch opcode[0] {
		case 0xFA: // AUX
			_, _, err = readAux(f)
			if err != nil {
				return fmt.Errorf("couldn't read AUX key-value pair: %w", err)
			}

		case 0xFB: // RESIZE DB
			// read length-encoded int for the size of hash table
			// read length-encoded int for the size of expire hash table
			dbLength, more, err := readEncodedLength(f)
			if err != nil {
				return fmt.Errorf("couldn't read hash table size: %w", err)
			}
			if more {
				return fmt.Errorf("length is encoded in the wrong way")
			}

			_, more, err = readEncodedLength(f)
			if err != nil {
				return fmt.Errorf("couldn't read expire hash table size: %w", err)
			}
			if more {
				return fmt.Errorf("length is encoded in the wrong way")
			}

			for i := 0; i < int(dbLength); i++ {
				dbopcode := make([]byte, 1)
				if err := read(f, dbopcode); err != nil {
					return fmt.Errorf("couldn't read db opcode: %w", err)
				}

				valueType := make([]byte, 1)
				expiration := uint64(0) // default: not expire (0)

				switch dbopcode[0] {
				case 0xFC: // EXPIRE MILLISECONDS - 8 byte length follows
					length := make([]byte, 8)
					if err := read(f, length); err != nil {
						return fmt.Errorf("couldn't read 8 byte length: %w", err)
					}

					expiration = binary.LittleEndian.Uint64(length)

					fmt.Println("expiration = ", expiration)

					if err := read(f, valueType); err != nil {
						return fmt.Errorf("couldn't read value type: %w", err)
					}

				case 0xFD: // EXPIRE SECONDS - 4 byte length follows
					length := make([]byte, 4)
					if err := read(f, length); err != nil {
						return fmt.Errorf("couldn't read 4 byte length: %w", err)
					}

					expiration = binary.LittleEndian.Uint64(length) * 1000 // second to millis

					fmt.Println("expiration = ", expiration)

					if err := read(f, valueType); err != nil {
						return fmt.Errorf("couldn't read value type: %w", err)
					}
				default:
					valueType[0] = dbopcode[0]
				}

				key, value, err := readKeyValue(f, valueType[0])
				if err != nil {
					return fmt.Errorf("couldn't read key and value: %w", err)
				}

				if expiration != 0 && uint64(time.Now().UnixMilli()) >= expiration {
					continue
				}

				cache.SetExpireAt(key, value, int64(expiration))
			}

		case 0xFE: // SELECT DB
			_, err = readDBNumber(f) // we are not using the db number at this moment.
			if err != nil {
				return fmt.Errorf("couldn't read DB number: %w", err)
			}

		case 0xFF: // EOF
			// TBD:
			_, err = readChecksum(f)
			if err != nil {
				return fmt.Errorf("couldn't read checksum: %w", err)
			}
			return nil
		}
	}
}

func read(f *os.File, buf []byte) error {
	toRead := len(buf)

	for {
		cnt, err := f.Read(buf)
		if err != nil {
			return fmt.Errorf("read failed: %w", err)

		}

		toRead = toRead - cnt
		if toRead == 0 {
			break
		}
	}
	return nil
}

func readHeader(f *os.File) (int, error) {
	buf := make([]byte, 9)
	if err := read(f, buf); err != nil {
		return 0, fmt.Errorf("couldn't read header: %w", err)
	}

	magic := string(buf[:5])
	if magic != "REDIS" {
		return 0, fmt.Errorf("the magic string is not REDIS: %s", magic)
	}

	ver, err := strconv.Atoi(string(buf[5:]))
	if err != nil {
		return 0, fmt.Errorf("couldn't read the version number: %w", err)
	}

	return ver, nil
}

func readAux(f *os.File) (string, string, error) {
	key, err := readEncodedString(f)
	if err != nil {
		return "", "", fmt.Errorf("couldn't read the key: %w", err)
	}

	fmt.Println("-- key =", string(key))

	value, err := readEncodedString(f)
	if err != nil {
		return "", "", fmt.Errorf("couldn't read value: %w", err)
	}

	fmt.Println("-- value =", string(value))

	return key, value, nil
}

func readDBNumber(f *os.File) (int, error) {
	number := make([]byte, 1)

	if err := read(f, number); err != nil {
		return 0, fmt.Errorf("couldn't read db number: %w", err)
	}

	return int(number[0]), nil
}

func readChecksum(f *os.File) (uint64, error) {
	checksum := make([]byte, 8)

	if err := read(f, checksum); err != nil {
		return 0, fmt.Errorf("couldn't read checksum: %w", err)
	}

	return binary.LittleEndian.Uint64(checksum), nil
}

// readEncodedLength returns the encoded length. If further processing is needed,
// information to determine the next step is returned as the first value, with second return value as true.
func readEncodedLength(f *os.File) (uint64, bool, error) {
	first := make([]byte, 1)

	if err := read(f, first); err != nil {
		return 0, false, fmt.Errorf("couldn't read the first byte of length encoded int: %w", err)
	}

	switch (0xC0 & first[0]) >> 6 {
	case 0: // The most significant 2 bits: 00 - The next 6 bit determines the length
		return uint64(0x3F & first[0]), false, nil
	case 1: // The most significant 2 bits: 01 - should read one additional byte: The combined 14 bits represents the length
		second := make([]byte, 1)
		if err := read(f, second); err != nil {
			return 0, false, fmt.Errorf("couldn't read the second byte of length encoded int: %w", err)
		}
		return uint64(0x3f&first[0])<<8 + uint64(second[0]), false, nil
	case 2: // The most significant 2 bits: 10 - Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
		length := make([]byte, 4)
		if err := read(f, length); err != nil {
			return 0, false, fmt.Errorf("couldn't read the second byte of length encoded int: %w", err)
		}
		return binary.LittleEndian.Uint64(length), false, nil
	case 3: // The most significant 2 bits: 11 - The remaining 6 bits determines the format. Maybe used to store numbers or strings.
		return uint64(0x3f & first[0]), true /* further processing needed */, nil
	}

	return 0, false, fmt.Errorf("case that shouldn't happen: %v", 0xC0&first[0])
}

func readEncodedString(f *os.File) (string, error) {
	length, more, err := readEncodedLength(f)
	if err != nil {
		return "", fmt.Errorf("couldn't read the value length: %w", err)
	}

	if !more {
		value := make([]byte, length)
		if err := read(f, value); err != nil {
			return "", fmt.Errorf("couldn't read value: %w", err)
		}

		return string(value), nil
	}

	switch length {
	case 0: // 8bit integer follows
		intg := make([]byte, 1)
		if err := read(f, intg); err != nil {
			return "", fmt.Errorf("cannot read 8bit integer: %w", err)
		}
		return string(intg), nil
	case 1: // 16bit integer follows
		intg := make([]byte, 2)
		if err := read(f, intg); err != nil {
			return "", fmt.Errorf("cannot read 16bit integer: %w", err)
		}
		return string(intg), nil
	case 2: // 32bit integer follows
		intg := make([]byte, 4)
		if err := read(f, intg); err != nil {
			return "", fmt.Errorf("cannot read 32bit integer: %w", err)
		}
		return string(intg), nil
	case 3: // compressed string follows
		clen, more, err := readEncodedLength(f)
		if err != nil {
			return "", fmt.Errorf("cannot read compressed string length: %w", err)
		}
		if more {
			return "", fmt.Errorf("unexpected length encoding")
		}

		ulen, more, err := readEncodedLength(f)
		if err != nil {
			return "", fmt.Errorf("cannot read uncompressed string length: %w", err)
		}
		if more {
			return "", fmt.Errorf("unexpected length encoding")
		}

		compressed := make([]byte, clen)
		if err := read(f, compressed); err != nil {
			return "", fmt.Errorf("cannot read compressed string: %w", err)
		}

		decompressed := make([]byte, ulen)
		if l, err := lzf.Decompress(compressed, decompressed); err != nil || uint64(l) != ulen {
			if err != nil {
				return "", fmt.Errorf("decompression failed: %w", err)
			}
			return "", fmt.Errorf("decompressed length different: %d != %d", l, ulen)
		}

		return string(decompressed), nil
	}

	return "", fmt.Errorf("unexpected value in length: %d", length)
}

func readKeyValue(f *os.File, valueType byte) (string, string, error) {
	// key is always string.
	key, err := readEncodedString(f)
	if err != nil {
		return "", "", fmt.Errorf("couldn't read key: %w", err)
	}

	if valueType != 0x00 {
		// we currently do not support value other than string.
		return "", "", fmt.Errorf("unimplemented")
	}

	value, err := readEncodedString(f)
	if err != nil {
		return "", "", fmt.Errorf("couldn't read key: %w", err)
	}

	fmt.Printf("key = %s, value = %s\n", key, value)

	return key, value, nil
}
