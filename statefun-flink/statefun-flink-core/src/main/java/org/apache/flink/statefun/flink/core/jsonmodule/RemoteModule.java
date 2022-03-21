/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.core.jsonmodule;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.statefun.flink.core.spi.ExtensionResolverAccessor.getExtensionResolver;

public final class RemoteModule implements StatefulFunctionModule {
  private static final Pattern replaceRegex = Pattern.compile("\\$\\{(.*?)\\}");
  private final List<JsonNode> componentNodes;

  RemoteModule(List<JsonNode> componentNodes) {
    this.componentNodes = Objects.requireNonNull(componentNodes);
  }

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder moduleBinder) {
    Map<String, String> systemPropsThenEnvVarsThenGlobalConfig =
        ParameterTool.fromSystemProperties()
            .mergeWith(
                ParameterTool.fromMap(System.getenv())
                    .mergeWith(ParameterTool.fromMap(globalConfiguration)))
            .toMap();

    parseComponentNodes(componentNodes)
        .forEach(
            component ->
                bindComponent(component, moduleBinder, systemPropsThenEnvVarsThenGlobalConfig));
  }

  private static List<ComponentJsonObject> parseComponentNodes(
      Iterable<? extends JsonNode> componentNodes) {
    return StreamSupport.stream(componentNodes.spliterator(), false)
        .filter(node -> !node.isNull())
        .map(ComponentJsonObject::new)
        .collect(Collectors.toList());
  }

  private static void bindComponent(
      ComponentJsonObject component, Binder moduleBinder, Map<String, String> configuration) {
    if (component.specJsonNode().isObject()) {
      resolveObject2((ObjectNode) component.specJsonNode(), configuration);
    }
    final ExtensionResolver extensionResolver = getExtensionResolver(moduleBinder);
    final ComponentBinder componentBinder =
        extensionResolver.resolveExtension(component.binderTypename(), ComponentBinder.class);
    componentBinder.bind(component, moduleBinder);
  }

  //  private static void resolvePlaceholders(
  //      JsonNode specJsonNode, Map<String, String> configuration) {
  //    resolvePlaceholders(null, null, specJsonNode, null, configuration);
  //  }

//  private static void resolvePlaceholders(
//      JsonNode parent,
//      String nodeName,
//      JsonNode node,
//      Map<String, String> config) {
//    if (node.isValueNode()) {
//      ((ObjectNode) parent).put(nodeName, resolveValueNode2((ValueNode) node, config));
//    } else if (node.isArray()) {
//      resolveArray((ArrayNode) node, config);
//    } else if (node.isObject()) {
//      resolveObject((ObjectNode) node, config);
//    }
//  }

  private static String resolveValueNode2(ValueNode node, Map<String, String> config) {
    if (node.textValue() == null) {
      return null;
    }
    StringBuffer stringBuffer = new StringBuffer();
    Matcher m = replaceRegex.matcher(node.textValue());
    while (m.find()) {
      if (config.containsKey(m.group(1))) {
        m.appendReplacement(stringBuffer, config.get(m.group(1)));
      }
    }
    m.appendTail(stringBuffer);

    return stringBuffer.toString();
  }

  //  private static void resolveValueNode(
  //      JsonNode parent,
  //      String nodeName,
  //      ValueNode node,
  //      Integer nodePositionInParentArray,
  //      Map<String, String> config) {
  //    if (node.textValue() != null) {
  //      StringBuffer stringBuffer = new StringBuffer();
  //      Matcher m = replaceRegex.matcher(node.textValue());
  //      while (m.find()) {
  //        if (config.containsKey(m.group(1))) {
  //          m.appendReplacement(stringBuffer, config.get(m.group(1)));
  //        }
  //      }
  //      m.appendTail(stringBuffer);
  //
  //      if (parent.isObject()) {
  //        ((ObjectNode) parent).put(nodeName, stringBuffer.toString());
  //      } else if (parent.isArray()) {
  //        ArrayNode anParent = ((ArrayNode) parent);
  //        anParent.set(nodePositionInParentArray, new TextNode(stringBuffer.toString()));
  //      }
  //    }
  //  }

//  private static void resolveObject(ObjectNode node, Map<String, String> config) {
//    node.fields()
//        .forEachRemaining(
//            kvp -> resolvePlaceholders(node, kvp.getKey(), kvp.getValue(), config));
//  }

  private static ObjectNode resolveObject2(ObjectNode node, Map<String, String> config) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fields(), 0), false)
        .map(resolve(config))
        .reduce(new ObjectNode(JsonNodeFactory.instance), (acc, tuple) -> {
              acc.put(tuple.getKey(), tuple.getValue());
              return acc;
            },
            (objectNode1, objectNode2) -> {
              throw new NotImplementedException("This reduce is not used with Parallel Streams");
            }
        );
  }

  private static Function<Map.Entry<String, JsonNode>, AbstractMap.SimpleEntry<String, JsonNode>> resolve(Map<String, String> config) {
    return kvp -> {
      if (kvp.getValue().isObject()) {
        return new AbstractMap.SimpleEntry<>(kvp.getKey(), resolveObject2((ObjectNode) kvp.getValue(), config));
      } else if (kvp.getValue().isArray()) {
        return new AbstractMap.SimpleEntry<>(kvp.getKey(), resolveArray2((ArrayNode) kvp.getValue(), config));
      } else if (kvp.getValue().isValueNode()) {
        return new AbstractMap.SimpleEntry<>(kvp.getKey(), new TextNode(resolveValueNode2((ValueNode) kvp.getValue(), config)));
      }

      // todo log warning? has not been replaced
      return new AbstractMap.SimpleEntry<>(kvp.getKey(), kvp.getValue());
    };
  }

  private static ArrayNode resolveArray2(ArrayNode node, Map<String, String> config) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fields(), 0), false)
        .map(resolve(config))
        .reduce(new ArrayNode(JsonNodeFactory.instance), (acc, tuple) -> {
              acc.add(tuple.getValue());
              return acc;
            },
            (objectNode1, objectNode2) -> {
              throw new NotImplementedException("This reduce is not used with Parallel Streams");
            }
        );
  }
//  private static void resolveArray(ArrayNode node, Map<String, String> config) {
//    for (int i = 0; i < node.size(); i++) {
//      if (node.get(i).isValueNode()) {
//        node.set(i, new TextNode(resolveValueNode2((ValueNode) node.get(i), config)));
//      } else if (node.get(i).isObject()) {
//        node.set(i, new TextNode(resolveObject((ValueNode) node.get(i), config);));
//        ((ObjectNode) node).put(node.get(i), stringBuffer.toString());
//      }
//    }
//  }
}
