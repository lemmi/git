package git

import (
	"bufio"
	"bytes"
	csha1 "crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func checkIdxVersion(r io.Reader, magic []byte, version uint32) error {
	var buf [8]byte
	n, err := io.ReadFull(r, buf[:])
	if err != nil {
		return err
	}
	if n < len(buf) {
		return errors.New("Unexpected EOF")
	}
	if !bytes.Equal(magic, buf[:4]) {
		return fmt.Errorf("Unknown magic byte %q, expected %q", buf[:4], magic)
	}
	if v := binary.BigEndian.Uint32(buf[4:]); v != version {
		return fmt.Errorf("Not a version %d idx file %q", version, v)
	}
	return nil
}

func isIdxOffsetValue64(v uint32) (bool, uint32) {
	// test wether MSB is set, return that and the other 31bits
	return v&(1<<31) > 0, v &^ (1 << 31)
}

func readIdxFile(path string) (*idxFile, error) {
	ifile := &idxFile{}
	ifile.indexpath = path
	ifile.packpath = path[0:len(path)-3] + "pack"

	idxf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()

	// Simulataniously calculate the checksum
	sha1sum := csha1.New()
	idx := io.TeeReader(idxf, sha1sum)

	// check magic byte and verion
	// level 0
	if err = checkIdxVersion(idx, []byte{255, 't', 'O', 'c'}, 2); err != nil {
		return nil, err
	}

	// read the complete fanout table
	// we only really use the last entry for the total number of entries
	// since we are putting the hashes into a map
	// level 1
	fanout := make([]uint32, 256)
	if err = binary.Read(idx, binary.BigEndian, fanout); err != nil {
		return nil, err
	}

	// read in all hashes. hashes should be in sorted order
	// level 2
	numObjects := int64(fanout[255])
	ids := make([]sha1, numObjects)

	for i := range ids {
		// read the next sha1 hash
		n, err := io.ReadFull(idx, ids[i][:])
		if n < len(ids[i]) {
			err = fmt.Errorf("Too short for sha1: %q", ids[i])
		}
		if err != nil {
			return nil, err
		}
	}

	// skip crc32 for now, only necessary for verifying the compressed
	// level 3
	if _, err := io.CopyN(ioutil.Discard, idx, 4*numObjects); err != nil {
		return nil, err
	}

	// the final offsets
	ifile.offsetValues = make(map[sha1]uint64, numObjects)

	// the short 31 bit offsets
	// MSB signals whether to use the other 31 bit as offset into the
	// packfile directly, or whether it's an index for the 64 bit
	// offsets in large packfiles (level 5)
	// level 4
	offsetValues32 := make([]uint32, numObjects)

	type link struct {
		id     sha1   // temporarily hold the hash, so we don't have to look it up again
		offset uint32 // offset into the 64 bit offset table
	}

	// temporarily hold hash <-> 64bit table index
	var idxOffsetValues64 []link

	// read the offsets and split out the large offsets
	for i := range offsetValues32 {
		var ov uint32
		if err != binary.Read(idx, binary.BigEndian, &ov) {
			return nil, err
		}
		if ok, ov := isIdxOffsetValue64(ov); ok {
			// MSB is set, This is an index into the 64 bit table.
			idxOffsetValues64 = append(idxOffsetValues64, link{ids[i], ov})
		} else {
			// MSB not set. We can add this offset directly.
			ifile.offsetValues[ids[i]] = uint64(ov)
		}
	}

	// assume there are as many 64bit entries as there are large offsets
	// level 5
	offsetValues64 := make([]uint64, len(idxOffsetValues64))

	// read in the complete table of large offsets
	if err != binary.Read(idx, binary.BigEndian, offsetValues64) {
		return nil, err
	}

	// look up the large offsets, put them into the map.
	for _, iov := range idxOffsetValues64 {
		if int(iov.offset) >= len(offsetValues64) {
			return nil, errors.New("Unexpected large index to 64bit index table")
		}
		ifile.offsetValues[iov.id] = offsetValues64[iov.offset]
	}

	// This is the sha1 hash for the associated pack file
	if _, err := io.CopyN(ioutil.Discard, idx, csha1.Size); err != nil {
		return nil, err
	}

	// finalize the sha1 of the idx file
	hashCalculated := sha1sum.Sum(nil)

	// we don't need to write anymore
	idx = nil
	// read the hash from the end of the file

	hashFile := make([]byte, csha1.Size)
	if _, err = io.ReadFull(idxf, hashFile); err != nil {
		return nil, err
	}

	if !bytes.Equal(hashFile, hashCalculated) {
		return nil, fmt.Errorf(`Chacksum missmatch. Got "%x", expected "%x"`, hashCalculated, hashFile)
	}

	// Not sure whether this should be done here.
	fi, err := os.Open(ifile.packpath)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	if err = checkIdxVersion(fi, []byte("PACK"), 2); err != nil {
		return nil, err
	}

	ifile.packversion = 2
	return ifile, nil
}

// If the object is stored in its own file (i.e not in a pack file),
// this function returns the full path to the object file.
// It does not test if the file exists.
func filepathFromSHA1(rootdir, sha1 string) string {
	return filepath.Join(rootdir, "objects", sha1[:2], sha1[2:])
}

// The object length in a packfile is a bit more difficult than
// just reading the bytes. The first byte has the length in its
// lowest four bits, and if bit 7 is set, it means 'more' bytes
// will follow. These are added to the »left side« of the length
func readLenInPackFile(buf []byte) (length int, advance int) {
	advance = 0
	shift := [...]byte{0, 4, 11, 18, 25, 32, 39, 46, 53, 60}
	length = int(buf[advance] & 0x0F)
	for buf[advance]&0x80 > 0 {
		advance += 1
		length += (int(buf[advance]&0x7F) << shift[advance])
	}
	advance++
	return
}

// Read from a pack file (given by path) at position offset. If this is a
// non-delta object, the (inflated) bytes are just returned, if the object
// is a deltafied-object, we have to apply the delta to base objects
// before hand.
func readObjectBytes(path string, indexfiles *map[string]*idxFile, offset uint64, sizeonly bool) (ot ObjectType, length int64, dataRc io.ReadCloser, err error) {
	offsetInt := int64(offset)
	file, err := os.Open(path)
	if err != nil {
		return
	}

	defer func() {
		if err != nil || sizeonly {
			if file != nil {
				file.Close()
			}
		}
	}()

	pos, err := file.Seek(offsetInt, io.SeekStart)
	if err != nil {
		return
	}

	if pos != offsetInt {
		err = errors.New("Seek went wrong")
		return
	}

	buf := make([]byte, 1024)
	n, err := file.Read(buf)
	if err != nil {
		return
	}

	if n == 0 {
		err = errors.New("Nothing read from pack file")
		return
	}

	ot = ObjectType(buf[0] & 0x70)

	l, p := readLenInPackFile(buf)
	pos = int64(p)
	length = int64(l)

	var baseObjectOffset uint64
	switch ot {
	case ObjectCommit, ObjectTree, ObjectBlob, ObjectTag:
		if sizeonly {
			// if we are only interested in the size of the object,
			// we don't need to do more expensive stuff
			return
		}

		_, err = file.Seek(offsetInt+pos, io.SeekStart)
		if err != nil {
			return
		}

		dataRc, err = readerDecompressed(file)
		if err != nil {
			return
		}
		dataRc = wrapReadCloser(io.LimitReader(dataRc, length), dataRc)
		return
		// data, err = readCompressedDataFromFile(file, offsetInt+pos, length)

	case 0x60:
		// DELTA_ENCODED object w/ offset to base
		// Read the offset first, then calculate the starting point
		// of the base object
		num := int64(buf[pos]) & 0x7f
		for buf[pos]&0x80 > 0 {
			pos = pos + 1
			num = ((num + 1) << 7) | int64(buf[pos]&0x7f)
		}
		baseObjectOffset = uint64(offsetInt - num)
		pos = pos + 1

	case 0x70:
		// DELTA_ENCODED object w/ base BINARY_OBJID
		var id sha1
		id, err = NewId(buf[pos : pos+20])
		if err != nil {
			return
		}

		pos = pos + 20

		f := (*indexfiles)[path[0:len(path)-4]+"idx"]
		var ok bool
		if baseObjectOffset, ok = f.offsetValues[id]; !ok {
			log.Fatal("not implemented yet")
			err = errors.New("base object is not exist")
			return
		}
	}

	var (
		base   []byte
		baseRc io.ReadCloser
	)
	ot, _, baseRc, err = readObjectBytes(path, indexfiles, baseObjectOffset, false)
	if err != nil {
		return
	}

	defer func() {
		baseRc.Close()
	}()

	base, err = ioutil.ReadAll(baseRc)
	if err != nil {
		return
	}

	_, err = file.Seek(offsetInt+pos, io.SeekStart)
	if err != nil {
		return
	}

	rc, err := readerDecompressed(file)
	if err != nil {
		return
	}

	zpos := 0
	// This is the length of the base object. Do we need to know it?
	_, bytesRead := readerLittleEndianBase128Number(rc)
	//log.Println(zpos, bytesRead)
	zpos += bytesRead

	resultObjectLength, bytesRead := readerLittleEndianBase128Number(rc)
	zpos += bytesRead

	if sizeonly {
		// if we are only interested in the size of the object,
		// we don't need to do more expensive stuff
		length = resultObjectLength
		return
	}

	br := &readAter{base}
	data, err := readerApplyDelta(br, rc, resultObjectLength)

	dataRc = newBufReadCloser(data)
	return
}

// Read the contents of the object file at path.
// Return the content type, the contents of the file and error, if any
func readObjectFile(path string, sizeonly bool) (ot ObjectType, length int64, dataRc io.ReadCloser, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, nil, err
	}
	dataRc, err = readerDecompressed(f)
	if err != nil {
		f.Close()
		return 0, 0, nil, err
	}

	// we need to buffer, otherwise Fscan can read too far
	dataRc = wrapReadCloser(bufio.NewReader(dataRc), dataRc)

	var t string
	_, err = fmt.Fscanf(dataRc, "%s %d\x00", &t, &length)

	if err != nil {
		dataRc.Close()
		return 0, 0, nil, err
	}

	if length < 0 {
		dataRc.Close()
		return 0, 0, nil, errors.New(`Negitive length of object file`)
	}

	// now wrap in LimitedReader to not read over the end
	dataRc = wrapReadCloser(io.LimitReader(dataRc, length), dataRc)

	switch t {
	case "blob":
		ot = ObjectBlob
	case "tree":
		ot = ObjectTree
	case "commit":
		ot = ObjectCommit
	case "tag":
		ot = ObjectTag
	default:
		dataRc.Close()
		return 0, 0, nil, fmt.Errorf(`Unknown object type: %q`, t)
	}

	if sizeonly {
		dataRc.Close()
		dataRc = nil
	}

	return ot, length, dataRc, nil
}
