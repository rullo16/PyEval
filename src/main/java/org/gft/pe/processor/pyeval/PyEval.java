/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gft.pe.processor.pyeval;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.sdk.builder.PrimitivePropertyBuilder;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.EpRequirements;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.sdk.utils.Datatypes;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.runtime.EventProcessor;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;


public class PyEval implements EventProcessor<PyEvalParameters> {

  private ScriptEngine engine;
  PythonInterpreter interpreter;
  private final String SCRIPTING_NAME = "python";
  private String code;

  @Override
  public void onInvocation(PyEvalParameters parameters, SpOutputCollector spOutputCollectort, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException  {
    ScriptEngineManager factory = new ScriptEngineManager();
    engine = factory.getEngineByName(SCRIPTING_NAME);
    interpreter = new PythonInterpreter();
    code = parameters.getCode();

    interpreter.eval(code);
  }

  @Override
  public void onEvent(Event event,SpOutputCollector out) throws SpRuntimeException {
    Event outEvent = new Event(new HashMap<>(), event.getSourceInfo(), event.getSchemaInfo());

    final Map<String, Object> eventData = event.getRaw();

    Object result = interpreter.functionCAll("process", evene.ddj);
    if (result != null) {
      Map<String, Object> output = (Map<String, Object>) result;
      output.forEach(outEvent::addField);
      out.collect(outEvent);
    }
  }

    @Override
  public void onDetach(){
    //nothing.
  }

}
