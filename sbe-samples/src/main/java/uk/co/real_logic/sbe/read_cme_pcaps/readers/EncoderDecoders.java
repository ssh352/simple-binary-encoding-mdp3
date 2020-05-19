package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.IrEncoder;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class EncoderDecoders {
    static void encodeSchema(final ByteBuffer byteBuffer, String schema_file) throws Exception {
        File initialFile = new File(schema_file);
        InputStream targetStream = new FileInputStream(initialFile);
        try (InputStream in = new BufferedInputStream(targetStream)) {
            final MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
            final Ir ir = new IrGenerator().generate(schema);
            try (IrEncoder irEncoder = new IrEncoder(byteBuffer, ir)) {
                irEncoder.encode();
            }
        }
    }

    static Ir decodeIr(final ByteBuffer buffer) {
        try (IrDecoder irDecoder = new IrDecoder(buffer)) {
            return irDecoder.decode();
        }
    }
}
