package org.gft.pe.processor.pyeval;

import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class PyEvalParameters extends EventProcessorBindingParams {

    private String code;

    public PyEvalParameters(DataProcessorInvocation graph, String code) {
        super(graph);
        this.code = code;
    }

    public String getCode(){
        return code;
    }
}
