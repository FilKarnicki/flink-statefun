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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.core.spi.ExtensionResolver;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.statefun.flink.core.spi.ExtensionResolverAccessor.getExtensionResolver;

public final class RemoteModule implements StatefulFunctionModule {
  private static final Pattern replaceRegex = Pattern.compile(".*(\\$\\{?([^}]+)}).*");
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
                    .mergeWith(
                        ParameterTool.fromMap(globalConfiguration)))
            .toMap();
    parseComponentNodes(componentNodes)
        .forEach(component -> bindComponent(component, moduleBinder, systemPropsThenEnvVarsThenGlobalConfig));
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
    resolvePlaceholders(component.specJsonNode(), configuration);
    final ExtensionResolver extensionResolver = getExtensionResolver(moduleBinder);
    final ComponentBinder componentBinder =
        extensionResolver.resolveExtension(component.binderTypename(), ComponentBinder.class);
    componentBinder.bind(component, moduleBinder);
  }

  private static void resolvePlaceholders(
      JsonNode specJsonNode, Map<String, String> configuration) {
    resolvePlaceholders(null, null, specJsonNode, null, configuration);
  }

  private static void resolvePlaceholders(
      JsonNode parent,
      String nodeName,
      JsonNode node,
      Integer nodePositionInParentArray,
      Map<String, String> config) {
    if (node.isValueNode()) {
      resolveValueNode(parent, nodeName, (ValueNode) node, nodePositionInParentArray, config);
    } else if (node.isArray()) {
      resolveArray((ArrayNode) node, config);
    } else if (node.isObject()) {
      resolveObject((ObjectNode) node, config);
    }
  }

  private static void resolveValueNode(
      JsonNode parent,
      String nodeName,
      ValueNode node,
      Integer nodePositionInParentArray,
      Map<String, String> config) {
    if (node.textValue() != null) {
      Matcher m = replaceRegex.matcher(node.textValue());
      if (m.matches() && config.containsKey(m.group(2))) {
        if (parent.isObject()) {
          ((ObjectNode) parent)
              .put(nodeName, node.textValue().replace(m.group(1), config.get(m.group(2))));
        } else if (parent.isArray()) {
          ArrayNode anParent = ((ArrayNode) parent);
          anParent.set(
              nodePositionInParentArray,
              new TextNode(node.textValue().replace(m.group(1), config.get(m.group(2)))));
        }
      }
    }
  }

  private static void resolveObject(ObjectNode node, Map<String, String> config) {
    node.fields()
        .forEachRemaining(
            kvp -> resolvePlaceholders(node, kvp.getKey(), kvp.getValue(), null, config));
  }

  private static void resolveArray(ArrayNode node, Map<String, String> config) {
    for (int i = 0; i < node.size(); i++) {
      resolvePlaceholders(node, null, node.get(i), i, config);
    }
  }
}
