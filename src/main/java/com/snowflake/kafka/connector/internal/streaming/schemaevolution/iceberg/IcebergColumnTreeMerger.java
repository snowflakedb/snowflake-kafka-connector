package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.base.Preconditions;
import com.snowflake.kafka.connector.internal.KCLogger;

public class IcebergColumnTreeMerger {

  private final KCLogger LOGGER = new KCLogger(IcebergColumnTreeMerger.class.getName());

  /**
   * Method designed for unstructured data types. Enhances already existing unstructured columns
   * with new subfields.
   */
  void merge(IcebergColumnTree currentTree, IcebergColumnTree treeWithNewType) {
    validate(currentTree, treeWithNewType);
    LOGGER.debug("Attempting to apply changes for column:" + currentTree.getColumnName());

    merge(currentTree.getRootNode(), treeWithNewType.getRootNode());
  }

  /** Method adds new children to a node. It does not change anything else. */
  private void merge(IcebergFieldNode currentNode, IcebergFieldNode nodeToMerge) {
    nodeToMerge.children.forEach(
        (key, node) -> {
          IcebergFieldNode currentNodesChild = currentNode.children.get(key);
          if (currentNodesChild == null) {
            currentNode.children.put(key, node);
          } else {
            merge(currentNodesChild, node);
          }
        });
  }

  private void validate(IcebergColumnTree currentTree, IcebergColumnTree treeWithNewType) {
    Preconditions.checkArgument(
        currentTree.getColumnName().equals(treeWithNewType.getColumnName()),
        "Error merging column schemas. Tried to merge schemas for two different columns");
  }
}
