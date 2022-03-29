package org.gft.pe.processor.pyeval;

import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class PyEvalController extends StandaloneEventProcessingDeclarer<PyEvalParameters> {

    private static final String PY_FUNCTION = "pyFunction";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.gft.pe.processor.pyeval")
                .category(DataProcessorType.SCRIPTING)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .requiredStream(StreamRequirementsBuilder.create()
                        .requiredProperty(EpRequirements.anyProperty())
                        .build())
                .requiredCodeblock(Labels.withId(PY_FUNCTION), CodeLanguage.Python)
                .outputStrategy(OutputStrategies.userDefined())
                .build();
    }

    @Override
    public ConfiguredEventProcessor<PyEvalParameters> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {
        String code = extractor.codeblockValue(PY_FUNCTION);
        PyEvalParameters parameters = new PyEvalParameters(graph,code);
        return new ConfiguredEventProcessor<>(parameters, PyEval::new);
    }
}
