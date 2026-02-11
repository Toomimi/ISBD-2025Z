# Tomy File Format

File format designed for storing columnar tabular data with compression support. The format uses a **Footer** to store metadata, allowing for easy streaming writes (without the need to buffer the entire content beforehand).

## File Structure

```text
[MagicBegin(4B)]          // "Tomy"
[Column Data...]
    [Col1 Data]  
    [Col2 Data]
    ...
[Metadata]
    [NumRows (varint)]
    [NumColumns (varint)]
    [Column Definitions...]
        [nameLength (varint) + name (bytes)]
        [columnType (1B)]
        [DataOffset (8B, LittleEndian)]   // Pointer to the start of column data in the file
        [CompressedSize (varint)]         // Size of the column data
[Metadata Offset (8B)]    // LittleEndian (int64) - pointer to the start of [Metadata] block
[MagicEnd(4B)]            // "EndT"
```

### Data Types

1.  **Int64 (0x01)**
    *   Compression: VLE + ZigZag + Delta Encoding.
2.  **Varchar (0x02)**
    *   Stored as Offsets + Data
    *   Compression:
        *   Offsets: VLE + Delta Encoding.
        *   Data: ZSTD

### Implementation Details

*   **Serialization**:
    1.  Write `MagicBegin`.
    2.  For each column: write compressed data, recording its `Offset` and `Size`.
    3.  At the end of the file, write the `Metadata` block (using collected offsets).
    4.  Write `Metadata Offset`.
    5.  Write `MagicEnd`.

*   **Deserialization**:
    1.  Check `MagicBegin`.
    2.  Jump to the end of the file (Minus footer size), read `VerifyMagicEnd` and `Metadata Offset`.
    3.  Jump to `Metadata Offset` and read column definitions.
    4.  With `DataOffset` for each column, read the compressed data and decompress it.
