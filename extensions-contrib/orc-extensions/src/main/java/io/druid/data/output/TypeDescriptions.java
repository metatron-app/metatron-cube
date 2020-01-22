/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.output;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is the description of the types in an ORC file.
 */
public class TypeDescriptions
{
  public enum Category
  {
    BOOLEAN("boolean", true),
    BYTE("tinyint", true),
    SHORT("smallint", true),
    INT("int", true),
    LONG("bigint", true),
    FLOAT("float", true),
    DOUBLE("double", true),
    STRING("string", true),
    DATE("date", true),
    TIMESTAMP("timestamp", true),
    BINARY("binary", true),
    DECIMAL("decimal", true),
    VARCHAR("varchar", true),
    CHAR("char", true),
    LIST("array", false),
    MAP("map", false),
    STRUCT("struct", false),
    UNION("uniontype", false);

    Category(String name, boolean isPrimitive)
    {
      this.name = name;
      this.isPrimitive = isPrimitive;
    }

    final boolean isPrimitive;
    final String name;

    public boolean isPrimitive()
    {
      return isPrimitive;
    }

    public String getName()
    {
      return name;
    }
  }

  public static Description createBoolean()
  {
    return new Description(Category.BOOLEAN);
  }

  public static Description createByte()
  {
    return new Description(Category.BYTE);
  }

  public static Description createShort()
  {
    return new Description(Category.SHORT);
  }

  public static Description createInt()
  {
    return new Description(Category.INT);
  }

  public static Description createLong()
  {
    return new Description(Category.LONG);
  }

  public static Description createFloat()
  {
    return new Description(Category.FLOAT);
  }

  public static Description createDouble()
  {
    return new Description(Category.DOUBLE);
  }

  public static Description createString()
  {
    return new Description(Category.STRING);
  }

  public static Description createDate()
  {
    return new Description(Category.DATE);
  }

  public static Description createTimestamp()
  {
    return new Description(Category.TIMESTAMP);
  }

  public static Description createBinary()
  {
    return new Description(Category.BINARY);
  }

  public static Description createDecimal()
  {
    return new Description(Category.DECIMAL);
  }

  static class StringPosition
  {
    final String value;
    int position;
    final int length;

    StringPosition(String value)
    {
      this.value = value;
      position = 0;
      length = value.length();
    }

    @Override
    public String toString()
    {
      StringBuilder buffer = new StringBuilder();
      buffer.append('\'');
      buffer.append(value, 0, position);
      buffer.append('^');
      buffer.append(value.substring(position));
      buffer.append('\'');
      return buffer.toString();
    }
  }

  static Category parseCategory(StringPosition source)
  {
    int start = source.position;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isLetter(ch)) {
        break;
      }
      source.position += 1;
    }
    if (source.position != start) {
      String word = source.value.substring(start, source.position).toLowerCase();
      for (Category cat : Category.values()) {
        if (cat.getName().equals(word)) {
          return cat;
        }
      }
    }
    throw new IllegalArgumentException("Can't parse category at " + source);
  }

  static int parseInt(StringPosition source)
  {
    int start = source.position;
    int result = 0;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isDigit(ch)) {
        break;
      }
      result = result * 10 + (ch - '0');
      source.position += 1;
    }
    if (source.position == start) {
      throw new IllegalArgumentException("Missing integer at " + source);
    }
    return result;
  }

  static String parseName(StringPosition source)
  {
    int start = source.position;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isLetterOrDigit(ch) && ch != '.' && ch != '_') {
        break;
      }
      source.position += 1;
    }
    if (source.position == start) {
      throw new IllegalArgumentException("Missing name at " + source);
    }
    return source.value.substring(start, source.position);
  }

  static void requireChar(StringPosition source, char required)
  {
    if (source.position >= source.length ||
        source.value.charAt(source.position) != required) {
      throw new IllegalArgumentException(
          "Missing required char '" +
          required + "' at " + source
      );
    }
    source.position += 1;
  }

  static boolean consumeChar(StringPosition source, char ch)
  {
    boolean result = source.position < source.length &&
                     source.value.charAt(source.position) == ch;
    if (result) {
      source.position += 1;
    }
    return result;
  }

  static void parseUnion(Description type, StringPosition source)
  {
    requireChar(source, '<');
    do {
      type.addUnionChild(parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
  }

  static void parseStruct(Description type, StringPosition source)
  {
    requireChar(source, '<');
    do {
      String fieldName = parseName(source);
      requireChar(source, ':');
      type.addField(fieldName, parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
  }

  static Description parseType(StringPosition source)
  {
    Description result = new Description(parseCategory(source));
    switch (result.getCategory()) {
      case BINARY:
      case BOOLEAN:
      case BYTE:
      case DATE:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case STRING:
      case TIMESTAMP:
        break;
      case CHAR:
      case VARCHAR:
        requireChar(source, '(');
        result.withMaxLength(parseInt(source));
        requireChar(source, ')');
        break;
      case DECIMAL: {
        requireChar(source, '(');
        int precision = parseInt(source);
        requireChar(source, ',');
        result.withScale(parseInt(source));
        result.withPrecision(precision);
        requireChar(source, ')');
        break;
      }
      case LIST:
        requireChar(source, '<');
        result.children.add(parseType(source));
        requireChar(source, '>');
        break;
      case MAP:
        requireChar(source, '<');
        result.children.add(parseType(source));
        requireChar(source, ',');
        result.children.add(parseType(source));
        requireChar(source, '>');
        break;
      case UNION:
        parseUnion(result, source);
        break;
      case STRUCT:
        parseStruct(result, source);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown type " +
            result.getCategory() + " at " + source
        );
    }
    return result;
  }

  public static String toOrcTypeString(String typeName)
  {
    if (!typeName.contains("long")) {
      return "struct<" + typeName + ">";
    }
    StringBuilder builder = new StringBuilder("struct<");
    for (String element : typeName.split(",")) {
      if (builder.length() > 7) {
        builder.append(',');
      }
      int index = element.indexOf(':');
      if (element.substring(index + 1).equals("long")) {
        builder.append(element, 0, index).append(':').append("bigint");
      } else {
        builder.append(element);
      }
    }
    return builder.append('>').toString();
  }

  /**
   * Parse TypeDescription from the Hive type names. This is the inverse
   * of TypeDescription.toString()
   *
   * @param typeName the name of the type
   *
   * @return a new TypeDescription or null if typeName was null
   *
   * @throws IllegalArgumentException if the string is badly formed
   */
  public static TypeDescription fromString(String typeName)
  {
    if (typeName == null) {
      return null;
    }
    StringPosition source = new StringPosition(typeName);
    Description result = parseType(source);
    if (source.position != source.length) {
      throw new IllegalArgumentException("Extra characters at " + source);
    }
    return toTypeDescription(result);
  }

  private static TypeDescription toTypeDescription(Description description)
  {
    TypeDescription result;
    switch (description.getCategory()) {
      case BINARY:
        result = TypeDescription.createBinary();
        break;
      case BOOLEAN:
        result = TypeDescription.createBoolean();
        break;
      case BYTE:
        result = TypeDescription.createByte();
        break;
      case DATE:
        result = TypeDescription.createDate();
        break;
      case DOUBLE:
        result = TypeDescription.createDouble();
        break;
      case FLOAT:
        result = TypeDescription.createFloat();
        break;
      case INT:
        result = TypeDescription.createInt();
        break;
      case LONG:
        result = TypeDescription.createLong();
        break;
      case SHORT:
        result = TypeDescription.createShort();
        break;
      case STRING:
        result = TypeDescription.createString();
        break;
      case TIMESTAMP:
        result = TypeDescription.createTimestamp();
        break;
      case CHAR:
        result = TypeDescription.createChar();
        result.withMaxLength(description.maxLength);
        break;
      case VARCHAR:
        result = TypeDescription.createVarchar();
        result.withMaxLength(description.maxLength);
        break;
      case DECIMAL:
        result = TypeDescription.createDecimal();
        result.withScale(description.scale);
        result.withPrecision(description.precision);
        break;
      case LIST:
        result = TypeDescription.createList(toTypeDescription(description.children.get(0)));
        break;
      case MAP:
        result = TypeDescription.createMap(
            toTypeDescription(description.getChildren().get(0)),
            toTypeDescription(description.getChildren().get(1))
        );
        break;
      case UNION:
        result = TypeDescription.createUnion();
        for (Description child : description.children) {
          result.addUnionChild(toTypeDescription(child));
        }
        break;
      case STRUCT:
        result = TypeDescription.createStruct();
        for (int i = 0; i < description.children.size(); i++) {
          result.addField(description.fieldNames.get(i), toTypeDescription(description.children.get(i)));
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + description.getCategory());
    }
    return result;
  }

  public static class Description
      implements Comparable<Description>, Serializable
  {
    private static final int MAX_PRECISION = 38;
    private static final int MAX_SCALE = 38;
    private static final int DEFAULT_PRECISION = 38;
    private static final int DEFAULT_SCALE = 10;
    private static final int DEFAULT_LENGTH = 256;

    @Override
    public int compareTo(Description other)
    {
      if (this == other) {
        return 0;
      } else if (other == null) {
        return -1;
      } else {
        int result = category.compareTo(other.category);
        if (result == 0) {
          switch (category) {
            case CHAR:
            case VARCHAR:
              return maxLength - other.maxLength;
            case DECIMAL:
              if (precision != other.precision) {
                return precision - other.precision;
              }
              return scale - other.scale;
            case UNION:
            case LIST:
            case MAP:
              if (children.size() != other.children.size()) {
                return children.size() - other.children.size();
              }
              for (int c = 0; result == 0 && c < children.size(); ++c) {
                result = children.get(c).compareTo(other.children.get(c));
              }
              break;
            case STRUCT:
              if (children.size() != other.children.size()) {
                return children.size() - other.children.size();
              }
              for (int c = 0; result == 0 && c < children.size(); ++c) {
                result = fieldNames.get(c).compareTo(other.fieldNames.get(c));
                if (result == 0) {
                  result = children.get(c).compareTo(other.children.get(c));
                }
              }
              break;
            default:
              // PASS
          }
        }
        return result;
      }
    }


    /**
     * For decimal types, set the precision.
     *
     * @param precision the new precision
     *
     * @return this
     */
    public Description withPrecision(int precision)
    {
      if (category != Category.DECIMAL) {
        throw new IllegalArgumentException(
            "precision is only allowed on decimal" +
            " and not " + category.name
        );
      } else if (precision < 1 || precision > MAX_PRECISION || scale > precision) {
        throw new IllegalArgumentException(
            "precision " + precision +
            " is out of range 1 .. " + scale
        );
      }
      this.precision = precision;
      return this;
    }

    /**
     * For decimal types, set the scale.
     *
     * @param scale the new scale
     *
     * @return this
     */
    public Description withScale(int scale)
    {
      if (category != Category.DECIMAL) {
        throw new IllegalArgumentException(
            "scale is only allowed on decimal" +
            " and not " + category.name
        );
      } else if (scale < 0 || scale > MAX_SCALE || scale > precision) {
        throw new IllegalArgumentException("scale is out of range at " + scale);
      }
      this.scale = scale;
      return this;
    }

    public static Description createVarchar()
    {
      return new Description(Category.VARCHAR);
    }

    public static Description createChar()
    {
      return new Description(Category.CHAR);
    }

    /**
     * Set the maximum length for char and varchar types.
     *
     * @param maxLength the maximum value
     *
     * @return this
     */
    public Description withMaxLength(int maxLength)
    {
      if (category != Category.VARCHAR && category != Category.CHAR) {
        throw new IllegalArgumentException(
            "maxLength is only allowed on char" +
            " and varchar and not " + category.name
        );
      }
      this.maxLength = maxLength;
      return this;
    }

    public static Description createList(Description childType)
    {
      Description result = new Description(Category.LIST);
      result.children.add(childType);
      childType.parent = result;
      return result;
    }

    public static Description createMap(
        Description keyType,
        Description valueType
    )
    {
      Description result = new Description(Category.MAP);
      result.children.add(keyType);
      result.children.add(valueType);
      keyType.parent = result;
      valueType.parent = result;
      return result;
    }

    public static Description createUnion()
    {
      return new Description(Category.UNION);
    }

    public static Description createStruct()
    {
      return new Description(Category.STRUCT);
    }

    /**
     * Add a child to a union type.
     *
     * @param child a new child type to add
     *
     * @return the union type.
     */
    public Description addUnionChild(Description child)
    {
      if (category != Category.UNION) {
        throw new IllegalArgumentException(
            "Can only add types to union type" +
            " and not " + category
        );
      }
      children.add(child);
      child.parent = this;
      return this;
    }

    /**
     * Add a field to a struct type as it is built.
     *
     * @param field     the field name
     * @param fieldType the type of the field
     *
     * @return the struct type
     */
    public Description addField(String field, Description fieldType)
    {
      if (category != Category.STRUCT) {
        throw new IllegalArgumentException(
            "Can only add fields to struct type" +
            " and not " + category
        );
      }
      fieldNames.add(field);
      children.add(fieldType);
      fieldType.parent = this;
      return this;
    }

    /**
     * Get the id for this type.
     * The first call will cause all of the the ids in tree to be assigned, so
     * it should not be called before the type is completely built.
     *
     * @return the sequential id
     */
    public int getId()
    {
      // if the id hasn't been assigned, assign all of the ids from the root
      if (id == -1) {
        Description root = this;
        while (root.parent != null) {
          root = root.parent;
        }
        root.assignIds(0);
      }
      return id;
    }

    @Override
    public Description clone()
    {
      Description result = new Description(category);
      result.maxLength = maxLength;
      result.precision = precision;
      result.scale = scale;
      if (fieldNames != null) {
        result.fieldNames.addAll(fieldNames);
      }
      if (children != null) {
        for (Description child : children) {
          Description clone = child.clone();
          clone.parent = result;
          result.children.add(clone);
        }
      }
      return result;
    }

    @Override
    public int hashCode()
    {
      long result = category.ordinal() * 4241 + maxLength + precision * 13 + scale;
      if (children != null) {
        for (Description child : children) {
          result = result * 6959 + child.hashCode();
        }
      }
      return (int) result;
    }

    @Override
    public boolean equals(Object other)
    {
      if (!(other instanceof Description)) {
        return false;
      }
      if (other == this) {
        return true;
      }
      Description castOther = (Description) other;
      if (category != castOther.category ||
          maxLength != castOther.maxLength ||
          scale != castOther.scale ||
          precision != castOther.precision) {
        return false;
      }
      if (children != null) {
        if (children.size() != castOther.children.size()) {
          return false;
        }
        for (int i = 0; i < children.size(); ++i) {
          if (!children.get(i).equals(castOther.children.get(i))) {
            return false;
          }
        }
      }
      if (category == Category.STRUCT) {
        for (int i = 0; i < fieldNames.size(); ++i) {
          if (!fieldNames.get(i).equals(castOther.fieldNames.get(i))) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Get the maximum id assigned to this type or its children.
     * The first call will cause all of the the ids in tree to be assigned, so
     * it should not be called before the type is completely built.
     *
     * @return the maximum id assigned under this type
     */
    public int getMaximumId()
    {
      // if the id hasn't been assigned, assign all of the ids from the root
      if (maxId == -1) {
        Description root = this;
        while (root.parent != null) {
          root = root.parent;
        }
        root.assignIds(0);
      }
      return maxId;
    }

    private ColumnVector createColumn(int maxSize)
    {
      switch (category) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DATE:
        case TIMESTAMP:
          return new LongColumnVector(maxSize);
        case FLOAT:
        case DOUBLE:
          return new DoubleColumnVector(maxSize);
        case DECIMAL:
          return new DecimalColumnVector(maxSize, precision, scale);
        case STRING:
        case BINARY:
        case CHAR:
        case VARCHAR:
          return new BytesColumnVector(maxSize);
        case STRUCT: {
          ColumnVector[] fieldVector = new ColumnVector[children.size()];
          for (int i = 0; i < fieldVector.length; ++i) {
            fieldVector[i] = children.get(i).createColumn(maxSize);
          }
          return new StructColumnVector(
              maxSize,
              fieldVector
          );
        }
        case UNION: {
          ColumnVector[] fieldVector = new ColumnVector[children.size()];
          for (int i = 0; i < fieldVector.length; ++i) {
            fieldVector[i] = children.get(i).createColumn(maxSize);
          }
          return new UnionColumnVector(
              maxSize,
              fieldVector
          );
        }
        case LIST:
          return new ListColumnVector(
              maxSize,
              children.get(0).createColumn(maxSize)
          );
        case MAP:
          return new MapColumnVector(
              maxSize,
              children.get(0).createColumn(maxSize),
              children.get(1).createColumn(maxSize)
          );
        default:
          throw new IllegalArgumentException("Unknown type " + category);
      }
    }

    public VectorizedRowBatch createRowBatch(int maxSize)
    {
      VectorizedRowBatch result;
      if (category == Category.STRUCT) {
        result = new VectorizedRowBatch(children.size(), maxSize);
        for (int i = 0; i < result.cols.length; ++i) {
          result.cols[i] = children.get(i).createColumn(maxSize);
        }
      } else {
        result = new VectorizedRowBatch(1, maxSize);
        result.cols[0] = createColumn(maxSize);
      }
      result.reset();
      return result;
    }

    public VectorizedRowBatch createRowBatch()
    {
      return createRowBatch(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Get the kind of this type.
     *
     * @return get the category for this type.
     */
    public Category getCategory()
    {
      return category;
    }

    /**
     * Get the maximum length of the type. Only used for char and varchar types.
     *
     * @return the maximum length of the string type
     */
    public int getMaxLength()
    {
      return maxLength;
    }

    /**
     * Get the precision of the decimal type.
     *
     * @return the number of digits for the precision.
     */
    public int getPrecision()
    {
      return precision;
    }

    /**
     * Get the scale of the decimal type.
     *
     * @return the number of digits for the scale.
     */
    public int getScale()
    {
      return scale;
    }

    /**
     * For struct types, get the list of field names.
     *
     * @return the list of field names.
     */
    public List<String> getFieldNames()
    {
      return Collections.unmodifiableList(fieldNames);
    }

    /**
     * Get the subtypes of this type.
     *
     * @return the list of children types
     */
    public List<Description> getChildren()
    {
      return children == null ? null : Collections.unmodifiableList(children);
    }

    /**
     * Assign ids to all of the nodes under this one.
     *
     * @param startId the lowest id to assign
     *
     * @return the next available id
     */
    private int assignIds(int startId)
    {
      id = startId++;
      if (children != null) {
        for (Description child : children) {
          startId = child.assignIds(startId);
        }
      }
      maxId = startId - 1;
      return startId;
    }

    private Description(Category category)
    {
      this.category = category;
      if (category.isPrimitive) {
        children = null;
      } else {
        children = new ArrayList<>();
      }
      if (category == Category.STRUCT) {
        fieldNames = new ArrayList<>();
      } else {
        fieldNames = null;
      }
    }

    private int id = -1;
    private int maxId = -1;
    private Description parent;
    private final Category category;
    private final List<Description> children;
    private final List<String> fieldNames;
    private int maxLength = DEFAULT_LENGTH;
    private int precision = DEFAULT_PRECISION;
    private int scale = DEFAULT_SCALE;

    public void printToBuffer(StringBuilder buffer)
    {
      buffer.append(category.name);
      switch (category) {
        case DECIMAL:
          buffer.append('(');
          buffer.append(precision);
          buffer.append(',');
          buffer.append(scale);
          buffer.append(')');
          break;
        case CHAR:
        case VARCHAR:
          buffer.append('(');
          buffer.append(maxLength);
          buffer.append(')');
          break;
        case LIST:
        case MAP:
        case UNION:
          buffer.append('<');
          for (int i = 0; i < children.size(); ++i) {
            if (i != 0) {
              buffer.append(',');
            }
            children.get(i).printToBuffer(buffer);
          }
          buffer.append('>');
          break;
        case STRUCT:
          buffer.append('<');
          for (int i = 0; i < children.size(); ++i) {
            if (i != 0) {
              buffer.append(',');
            }
            buffer.append(fieldNames.get(i));
            buffer.append(':');
            children.get(i).printToBuffer(buffer);
          }
          buffer.append('>');
          break;
        default:
          break;
      }
    }

    public String toString()
    {
      StringBuilder buffer = new StringBuilder();
      printToBuffer(buffer);
      return buffer.toString();
    }

    private void printJsonToBuffer(
        String prefix, StringBuilder buffer,
        int indent
    )
    {
      for (int i = 0; i < indent; ++i) {
        buffer.append(' ');
      }
      buffer.append(prefix);
      buffer.append("{\"category\": \"");
      buffer.append(category.name);
      buffer.append("\", \"id\": ");
      buffer.append(getId());
      buffer.append(", \"max\": ");
      buffer.append(maxId);
      switch (category) {
        case DECIMAL:
          buffer.append(", \"precision\": ");
          buffer.append(precision);
          buffer.append(", \"scale\": ");
          buffer.append(scale);
          break;
        case CHAR:
        case VARCHAR:
          buffer.append(", \"length\": ");
          buffer.append(maxLength);
          break;
        case LIST:
        case MAP:
        case UNION:
          buffer.append(", \"children\": [");
          for (int i = 0; i < children.size(); ++i) {
            buffer.append('\n');
            children.get(i).printJsonToBuffer("", buffer, indent + 2);
            if (i != children.size() - 1) {
              buffer.append(',');
            }
          }
          buffer.append("]");
          break;
        case STRUCT:
          buffer.append(", \"fields\": [");
          for (int i = 0; i < children.size(); ++i) {
            buffer.append('\n');
            children.get(i).printJsonToBuffer(
                "\"" + fieldNames.get(i) + "\": ",
                buffer, indent + 2
            );
            if (i != children.size() - 1) {
              buffer.append(',');
            }
          }
          buffer.append(']');
          break;
        default:
          break;
      }
      buffer.append('}');
    }

    public String toJson()
    {
      StringBuilder buffer = new StringBuilder();
      printJsonToBuffer("", buffer, 0);
      return buffer.toString();
    }
  }
}