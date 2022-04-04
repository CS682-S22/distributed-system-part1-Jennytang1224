// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: protos/heartBeatMessage.proto

package dsd.pubsub.protos;

public final class HeartBeatMessage {
  private HeartBeatMessage() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface HeartBeatOrBuilder extends
      // @@protoc_insertion_point(interface_extends:HeartBeat)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>int32 senderID = 1;</code>
     * @return The senderID.
     */
    int getSenderID();

    /**
     * <code>int32 numOfRetires = 2;</code>
     * @return The numOfRetires.
     */
    int getNumOfRetires();
  }
  /**
   * Protobuf type {@code HeartBeat}
   */
  public static final class HeartBeat extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:HeartBeat)
      HeartBeatOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use HeartBeat.newBuilder() to construct.
    private HeartBeat(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private HeartBeat() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new HeartBeat();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private HeartBeat(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 8: {

              senderID_ = input.readInt32();
              break;
            }
            case 16: {

              numOfRetires_ = input.readInt32();
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return dsd.pubsub.protos.HeartBeatMessage.internal_static_HeartBeat_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return dsd.pubsub.protos.HeartBeatMessage.internal_static_HeartBeat_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              dsd.pubsub.protos.HeartBeatMessage.HeartBeat.class, dsd.pubsub.protos.HeartBeatMessage.HeartBeat.Builder.class);
    }

    public static final int SENDERID_FIELD_NUMBER = 1;
    private int senderID_;
    /**
     * <code>int32 senderID = 1;</code>
     * @return The senderID.
     */
    @java.lang.Override
    public int getSenderID() {
      return senderID_;
    }

    public static final int NUMOFRETIRES_FIELD_NUMBER = 2;
    private int numOfRetires_;
    /**
     * <code>int32 numOfRetires = 2;</code>
     * @return The numOfRetires.
     */
    @java.lang.Override
    public int getNumOfRetires() {
      return numOfRetires_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (senderID_ != 0) {
        output.writeInt32(1, senderID_);
      }
      if (numOfRetires_ != 0) {
        output.writeInt32(2, numOfRetires_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (senderID_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, senderID_);
      }
      if (numOfRetires_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, numOfRetires_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof dsd.pubsub.protos.HeartBeatMessage.HeartBeat)) {
        return super.equals(obj);
      }
      dsd.pubsub.protos.HeartBeatMessage.HeartBeat other = (dsd.pubsub.protos.HeartBeatMessage.HeartBeat) obj;

      if (getSenderID()
          != other.getSenderID()) return false;
      if (getNumOfRetires()
          != other.getNumOfRetires()) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + SENDERID_FIELD_NUMBER;
      hash = (53 * hash) + getSenderID();
      hash = (37 * hash) + NUMOFRETIRES_FIELD_NUMBER;
      hash = (53 * hash) + getNumOfRetires();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(dsd.pubsub.protos.HeartBeatMessage.HeartBeat prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code HeartBeat}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:HeartBeat)
        dsd.pubsub.protos.HeartBeatMessage.HeartBeatOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return dsd.pubsub.protos.HeartBeatMessage.internal_static_HeartBeat_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return dsd.pubsub.protos.HeartBeatMessage.internal_static_HeartBeat_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                dsd.pubsub.protos.HeartBeatMessage.HeartBeat.class, dsd.pubsub.protos.HeartBeatMessage.HeartBeat.Builder.class);
      }

      // Construct using dsd.pubsub.protos.HeartBeatMessage.HeartBeat.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        senderID_ = 0;

        numOfRetires_ = 0;

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return dsd.pubsub.protos.HeartBeatMessage.internal_static_HeartBeat_descriptor;
      }

      @java.lang.Override
      public dsd.pubsub.protos.HeartBeatMessage.HeartBeat getDefaultInstanceForType() {
        return dsd.pubsub.protos.HeartBeatMessage.HeartBeat.getDefaultInstance();
      }

      @java.lang.Override
      public dsd.pubsub.protos.HeartBeatMessage.HeartBeat build() {
        dsd.pubsub.protos.HeartBeatMessage.HeartBeat result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public dsd.pubsub.protos.HeartBeatMessage.HeartBeat buildPartial() {
        dsd.pubsub.protos.HeartBeatMessage.HeartBeat result = new dsd.pubsub.protos.HeartBeatMessage.HeartBeat(this);
        result.senderID_ = senderID_;
        result.numOfRetires_ = numOfRetires_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof dsd.pubsub.protos.HeartBeatMessage.HeartBeat) {
          return mergeFrom((dsd.pubsub.protos.HeartBeatMessage.HeartBeat)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(dsd.pubsub.protos.HeartBeatMessage.HeartBeat other) {
        if (other == dsd.pubsub.protos.HeartBeatMessage.HeartBeat.getDefaultInstance()) return this;
        if (other.getSenderID() != 0) {
          setSenderID(other.getSenderID());
        }
        if (other.getNumOfRetires() != 0) {
          setNumOfRetires(other.getNumOfRetires());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        dsd.pubsub.protos.HeartBeatMessage.HeartBeat parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (dsd.pubsub.protos.HeartBeatMessage.HeartBeat) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private int senderID_ ;
      /**
       * <code>int32 senderID = 1;</code>
       * @return The senderID.
       */
      @java.lang.Override
      public int getSenderID() {
        return senderID_;
      }
      /**
       * <code>int32 senderID = 1;</code>
       * @param value The senderID to set.
       * @return This builder for chaining.
       */
      public Builder setSenderID(int value) {
        
        senderID_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 senderID = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearSenderID() {
        
        senderID_ = 0;
        onChanged();
        return this;
      }

      private int numOfRetires_ ;
      /**
       * <code>int32 numOfRetires = 2;</code>
       * @return The numOfRetires.
       */
      @java.lang.Override
      public int getNumOfRetires() {
        return numOfRetires_;
      }
      /**
       * <code>int32 numOfRetires = 2;</code>
       * @param value The numOfRetires to set.
       * @return This builder for chaining.
       */
      public Builder setNumOfRetires(int value) {
        
        numOfRetires_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 numOfRetires = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearNumOfRetires() {
        
        numOfRetires_ = 0;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:HeartBeat)
    }

    // @@protoc_insertion_point(class_scope:HeartBeat)
    private static final dsd.pubsub.protos.HeartBeatMessage.HeartBeat DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new dsd.pubsub.protos.HeartBeatMessage.HeartBeat();
    }

    public static dsd.pubsub.protos.HeartBeatMessage.HeartBeat getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<HeartBeat>
        PARSER = new com.google.protobuf.AbstractParser<HeartBeat>() {
      @java.lang.Override
      public HeartBeat parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new HeartBeat(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<HeartBeat> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<HeartBeat> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public dsd.pubsub.protos.HeartBeatMessage.HeartBeat getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_HeartBeat_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_HeartBeat_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\035protos/heartBeatMessage.proto\"3\n\tHeart" +
      "Beat\022\020\n\010senderID\030\001 \001(\005\022\024\n\014numOfRetires\030\002" +
      " \001(\005B%\n\021dsd.pubsub.protosB\020HeartBeatMess" +
      "ageb\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_HeartBeat_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_HeartBeat_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_HeartBeat_descriptor,
        new java.lang.String[] { "SenderID", "NumOfRetires", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}