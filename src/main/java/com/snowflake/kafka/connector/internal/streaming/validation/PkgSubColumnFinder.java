/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import static net.snowflake.ingest.utils.Utils.concatDotPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/** Helper class to find all leaf columns in an immutable schema given a fieldId. */
public class PkgSubColumnFinder {

  /**
   * Helper class to store the start and end index of the interval of leaf columns of a node in the
   * list and the dot path of the node.
   */
  static class SubtreeInfo {
    private final int startTag;
    private final int endTag;
    private final String dotPath;
    private final List<String> path;

    /**
     * @param startTag Start index of the leaf column in the list.
     * @param endTag End index of the leaf column in the list.
     * @param dotPath Dot path of the node.
     */
    public SubtreeInfo(int startTag, int endTag, String dotPath, List<String> path) {
      this.startTag = startTag;
      this.endTag = endTag;
      this.dotPath = dotPath;
      this.path = path;
    }

    public int startTag() {
      return startTag;
    }

    public int endTag() {
      return endTag;
    }

    public String dotPath() {
      return dotPath;
    }

    public List<String> path() {
      return path;
    }
  }

  /* A list to store all leaf columns field id in preorder traversal. */
  private final List<String> list;

  /* A map to cache query result, avoid recursive query during runtime. */
  private final Map<Type.ID, SubtreeInfo> accessMap;

  public PkgSubColumnFinder(MessageType schema) {
    accessMap = new HashMap<>();
    list = new ArrayList<>();
    build(schema, null);
  }

  /**
   * Get all leaf sub-column's field id of a node in the schema.
   *
   * @param id Field id of the node
   * @return List of sub-column's field id
   */
  public List<String> getSubColumns(Type.ID id) {
    if (!accessMap.containsKey(id)) {
      throw new IllegalArgumentException(String.format("Field %s not found in schema", id));
    }
    SubtreeInfo interval = accessMap.get(id);
    return Collections.unmodifiableList(list.subList(interval.startTag, interval.endTag));
  }

  /**
   * Get the dot path of a node in the schema.
   *
   * @param id Field ID of the node
   * @return Dot path of the node
   */
  public String getDotPath(Type.ID id) {
    if (!accessMap.containsKey(id)) {
      throw new IllegalArgumentException(String.format("Field %s not found in schema", id));
    }
    return accessMap.get(id).dotPath;
  }

  /**
   * Get the path of a node in the schema.
   *
   * @param id Field ID of the node
   * @return Path of the node
   */
  public List<String> getPath(Type.ID id) {
    if (!accessMap.containsKey(id)) {
      throw new IllegalArgumentException(String.format("Field %s not found in schema", id));
    }
    return Collections.unmodifiableList(accessMap.get(id).path);
  }

  /**
   * Build the list of leaf columns in preorder traversal and the map of field id to the interval of
   * a node's leaf columns in the list.
   *
   * @param node The node to build the list and accessMap for its children.
   * @param currentPath The current path of the node. This serve like a stack and used to keep track
   *     of the dot path of current node. Always pop the last element of the path at the end of the
   *     recursion
   */
  private void build(Type node, List<String> currentPath) {
    if (currentPath == null) {
      /* Ignore root node type name (bdec or schema) */
      currentPath = new ArrayList<>();
    } else {
      currentPath.add(node.getName());
    }

    int startTag = list.size();
    if (!node.isPrimitive()) {
      for (Type child : node.asGroupType().getFields()) {
        build(child, currentPath);
      }
    } else {
      list.add(node.getId().toString());
    }
    if (!currentPath.isEmpty() && node.getId() != null) {
      accessMap.put(
          node.getId(),
          new SubtreeInfo(
              startTag,
              list.size(),
              concatDotPath(currentPath.toArray(new String[0])),
              new ArrayList<>(currentPath)));
    }
    if (!currentPath.isEmpty()) {
      /* Remove the last element of the path at the end of recursion. */
      currentPath.remove(currentPath.size() - 1);
    }
  }
}
