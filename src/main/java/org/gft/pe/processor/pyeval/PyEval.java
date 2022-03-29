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
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;


public class PyEval implements EventProcessor<PyEvalParameters> {

  private Context polyglot;
  private Value function;

  @Override
  public void onInvocation(PyEvalParameters parameters, SpOutputCollector spOutputCollectort, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException  {

    polyglot = Context.create();
    String code = parameters.getCode();
    function = polyglot.eval("python", "(" + code + ")");
  }

  @Override
  public void onEvent(Event event,SpOutputCollector out) throws SpRuntimeException{
    Event outEvent = new Event(new HashMap<>(), event.getSourceInfo(), event.getSchemaInfo());

    try{

      final Map<String,Object> eventData = event.getRaw();

      Object result = function.execute(ProxyObject.fromMap(eventData));
      Map<String,Object> resultEvent = ((Value) result).as(java.util.Map.class);
      if (resultEvent != null){
        resultEvent.forEach(outEvent::addField);
        out.collect(outEvent);
      }

    } catch (ClassCastException e) {
      throw new SpRuntimeException("'process' method must return a map with new event data.");
    }
  }

  @Override
  public void onDetach(){
    //nothing.
  }

}
