package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class ProofPipelineInputDecoderTest {
    private final ProofPipelineInputDecoder decoder = new ProofPipelineInputDecoder(new ObjectMapper());

    @Test
    void preservesTheLegacyTypedInputAndDiagnosticFlags() {
        assertThat(decoder.decode("{\"request\":\"approved\",\"deadlineProbe\":true,\"callbackExpiryProbe\":true}"))
            .isEqualTo(new ProofPipelineInput("approved", true, true));
    }

    @Test
    void malformedAndNullJsonCannotSupplyATypedInput() {
        for (String input : java.util.List.of("{invalid", "null")) {
            assertThatThrownBy(() -> decoder.decode(input))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("inputJson must contain a valid proof pipeline input");
        }
    }
}
