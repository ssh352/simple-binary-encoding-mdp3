package uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.decoders;

import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.ir.Token;

public class BufferDecoders {

    private static PrimitiveValue constOrNotPresentValue(final Token token, final int actingVersion) {
        if (token.isConstantEncoding()) {
            return token.encoding().constValue();
        } else if (token.isOptionalEncoding() && actingVersion < token.version()) {
            return token.encoding().applicableNullValue();
        }

        return null;
    }
}
