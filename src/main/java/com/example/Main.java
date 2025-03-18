package com.example;

import example.avro.Example;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.time.Instant;

public class Main {
    public static void main(String[] args) throws Exception {
        // here we use reflection to determine the schema
        Schema schema = ReflectData.get().getSchema(Example.class);
        System.out.println("schema = " + schema.toString(true));


        System.out.println("Writing example.avro");
        File file = new File("example.avro");
        DatumWriter<Example> writer = new ReflectDatumWriter<>(Example.class);
        try (DataFileWriter<Example> out = new DataFileWriter<>(writer)
                .setCodec(CodecFactory.deflateCodec(9))
                .create(schema, file)) {

            out.append(Example.newBuilder().setExample("example")
                    .setTest(Instant.now())
                    .build());
        }
        System.out.println("Reading example2.avro");
        File file2 = new File("example.avro");
        DatumReader<Example> reader = new ReflectDatumReader<>(Example.class);
        try (DataFileReader<Example> in = new DataFileReader<>(file2, reader)) {
            for (Example example : in) {
                System.out.println(example.getExample());
            }
        }

        System.out.println("Writing Now a bad example!");

        DatumWriter<Example> writer2 = new ReflectDatumWriter<>(Example.class);
        try (DataFileWriter<Example> out = new DataFileWriter<>(writer2)
                .setCodec(CodecFactory.deflateCodec(9))
                .create(schema, file)) {

            out.append(Example.newBuilder().setExample("example")
                    .setTest(Instant.now())
                    .setValueDate(Instant.now())
                    .build());
        }

    }
}