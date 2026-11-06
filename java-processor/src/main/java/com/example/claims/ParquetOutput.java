package com.example.claims;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Parquet writer for the merged claims output.
 *
 * Why Parquet helps here:
 *  - Columnar + typed: no text formatting/parsing of 730M rows.
 *  - Snappy compression is ~5-10x faster than gzip AND columnar encoding
 *    (dictionary + RLE) makes the output smaller than gzipped CSV.
 *  - Column pruning on read: downstream queries skip unused columns.
 */
public final class ParquetOutput implements AutoCloseable {

    // 21-column merged schema (claims + member attrs + provider attrs)
    public static final Schema SCHEMA = SchemaBuilder.record("MergedClaim").fields()
            .optionalString("claim_id")
            .optionalString("member_id")
            .optionalString("provider_id")
            .optionalString("claim_date")
            .optionalString("service_code")
            .optionalString("diagnosis_code")
            .optionalDouble("billed_amount")
            .optionalDouble("paid_amount")
            .optionalString("claim_status")
            .optionalString("state")
            .optionalString("member_dob")
            .optionalString("source_system")
            .optionalString("member_first_name")
            .optionalString("member_last_name")
            .optionalString("member_gender")
            .optionalString("member_plan_type")
            .optionalDouble("member_risk_score")
            .optionalString("provider_name")
            .optionalString("provider_specialty")
            .optionalString("provider_npi")
            .optionalString("provider_network_status")
            .endRecord();

    private final ParquetWriter<GenericRecord> writer;
    private final GenericRecord record = new GenericData.Record(SCHEMA);

    /**
     * Writes to a plain local OutputStream via Parquet's OutputFile API,
     * bypassing Hadoop's FileSystem entirely (avoids the HADOOP_HOME /
     * native-lib hang on macOS Apple Silicon).
     */
    public ParquetOutput(Path file) throws IOException {
        OutputStream out = new java.io.BufferedOutputStream(Files.newOutputStream(file), 1 << 20);
        OutputFile outputFile = new OutputFile() {
            @Override public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long pos = 0;
                    @Override public long getPos() { return pos; }
                    @Override public void write(int b) throws IOException { out.write(b); pos++; }
                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len); pos += len;
                    }
                    @Override public void close() throws IOException { out.close(); }
                };
            }
            @Override public PositionOutputStream createOrOverwrite(long blockSizeHint) { return create(blockSizeHint); }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 128L * 1024 * 1024; }
        };
        this.writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(128L * 1024 * 1024) // 128 MB row groups
                .withPageSize(1 << 20)
                .build();
    }

    /** Write a merged row. Pass null for missing values. */
    public void write(String claimId, String memberId, String providerId,
                      String claimDate, String serviceCode, String diagCode,
                      Double billed, Double paid, String status, String state,
                      String dob, String sourceSystem,
                      String mFirst, String mLast, String mGender, String mPlan, Double mRisk,
                      String pName, String pSpecialty, String pNpi, String pNetwork) throws IOException {
        // Reused: ParquetWriter.write() shreds the record into column buffers
        // synchronously, so it is safe to overwrite every field on the next row.
        GenericRecord r = record;
        put(r, 0, claimId); put(r, 1, memberId); put(r, 2, providerId);
        put(r, 3, claimDate); put(r, 4, serviceCode); put(r, 5, diagCode);
        r.put(6, billed); r.put(7, paid);
        put(r, 8, status); put(r, 9, state); put(r, 10, dob); put(r, 11, sourceSystem);
        put(r, 12, mFirst); put(r, 13, mLast); put(r, 14, mGender); put(r, 15, mPlan);
        r.put(16, mRisk);
        put(r, 17, pName); put(r, 18, pSpecialty); put(r, 19, pNpi); put(r, 20, pNetwork);
        writer.write(r);
    }

    private static void put(GenericRecord r, int i, String v) {
        r.put(i, (v != null && !v.isEmpty()) ? v : null);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
