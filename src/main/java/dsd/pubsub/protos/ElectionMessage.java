// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: protos/electionMessage.proto

package dsd.pubsub.protos;

public final class ElectionMessage {
  private ElectionMessage() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface ElectionOrBuilder extends
      // @@protoc_insertion_point(interface_extends:Election)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>int32 senderID = 1;</code>
     * @return The senderID.
     */
    int getSenderID();

    /**
     * <code>int32 winnerID = 2;</code>
     * @return The winnerID.
     */
    int getWinnerID();
  }
  /**
   * Protobuf type {@code Election}
   */
  public static final class Election extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:Election)
      ElectionOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use Election.newBuilder() to construct.
    private Election(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private Election() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new Election();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private Election(
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

              winnerID_ = input.readInt32();
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
      return dsd.pubsub.protos.ElectionMessage.internal_static_Election_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return dsd.pubsub.protos.ElectionMessage.internal_static_Election_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              dsd.pubsub.protos.ElectionMessage.Election.class, dsd.pubsub.protos.ElectionMessage.Election.Builder.class);
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

    public static final int WINNERID_FIELD_NUMBER = 2;
    private int winnerID_;
    /**
     * <code>int32 winnerID = 2;</code>
     * @return The winnerID.
     */
    @java.lang.Override
    public int getWinnerID() {
      return winnerID_;
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
      if (winnerID_ != 0) {
        output.writeInt32(2, winnerID_);
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
      if (winnerID_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, winnerID_);
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
      if (!(obj instanceof dsd.pubsub.protos.ElectionMessage.Election)) {
        return super.equals(obj);
      }
      dsd.pubsub.protos.ElectionMessage.Election other = (dsd.pubsub.protos.ElectionMessage.Election) obj;

      if (getSenderID()
          != other.getSenderID()) return false;
      if (getWinnerID()
          != other.getWinnerID()) return false;
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
      hash = (37 * hash) + WINNERID_FIELD_NUMBER;
      hash = (53 * hash) + getWinnerID();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static dsd.pubsub.protos.ElectionMessage.Election parseFrom(
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
    public static Builder newBuilder(dsd.pubsub.protos.ElectionMessage.Election prototype) {
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
     * Protobuf type {@code Election}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:Election)
        dsd.pubsub.protos.ElectionMessage.ElectionOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return dsd.pubsub.protos.ElectionMessage.internal_static_Election_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return dsd.pubsub.protos.ElectionMessage.internal_static_Election_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                dsd.pubsub.protos.ElectionMessage.Election.class, dsd.pubsub.protos.ElectionMessage.Election.Builder.class);
      }

      // Construct using dsd.pubsub.protos.ElectionMessage.Election.newBuilder()
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

        winnerID_ = 0;

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return dsd.pubsub.protos.ElectionMessage.internal_static_Election_descriptor;
      }

      @java.lang.Override
      public dsd.pubsub.protos.ElectionMessage.Election getDefaultInstanceForType() {
        return dsd.pubsub.protos.ElectionMessage.Election.getDefaultInstance();
      }

      @java.lang.Override
      public dsd.pubsub.protos.ElectionMessage.Election build() {
        dsd.pubsub.protos.ElectionMessage.Election result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public dsd.pubsub.protos.ElectionMessage.Election buildPartial() {
        dsd.pubsub.protos.ElectionMessage.Election result = new dsd.pubsub.protos.ElectionMessage.Election(this);
        result.senderID_ = senderID_;
        result.winnerID_ = winnerID_;
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
        if (other instanceof dsd.pubsub.protos.ElectionMessage.Election) {
          return mergeFrom((dsd.pubsub.protos.ElectionMessage.Election)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(dsd.pubsub.protos.ElectionMessage.Election other) {
        if (other == dsd.pubsub.protos.ElectionMessage.Election.getDefaultInstance()) return this;
        if (other.getSenderID() != 0) {
          setSenderID(other.getSenderID());
        }
        if (other.getWinnerID() != 0) {
          setWinnerID(other.getWinnerID());
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
        dsd.pubsub.protos.ElectionMessage.Election parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (dsd.pubsub.protos.ElectionMessage.Election) e.getUnfinishedMessage();
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

      private int winnerID_ ;
      /**
       * <code>int32 winnerID = 2;</code>
       * @return The winnerID.
       */
      @java.lang.Override
      public int getWinnerID() {
        return winnerID_;
      }
      /**
       * <code>int32 winnerID = 2;</code>
       * @param value The winnerID to set.
       * @return This builder for chaining.
       */
      public Builder setWinnerID(int value) {
        
        winnerID_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 winnerID = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearWinnerID() {
        
        winnerID_ = 0;
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


      // @@protoc_insertion_point(builder_scope:Election)
    }

    // @@protoc_insertion_point(class_scope:Election)
    private static final dsd.pubsub.protos.ElectionMessage.Election DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new dsd.pubsub.protos.ElectionMessage.Election();
    }

    public static dsd.pubsub.protos.ElectionMessage.Election getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<Election>
        PARSER = new com.google.protobuf.AbstractParser<Election>() {
      @java.lang.Override
      public Election parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Election(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<Election> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Election> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public dsd.pubsub.protos.ElectionMessage.Election getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Election_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Election_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\034protos/electionMessage.proto\".\n\010Electi" +
      "on\022\020\n\010senderID\030\001 \001(\005\022\020\n\010winnerID\030\002 \001(\005B$" +
      "\n\021dsd.pubsub.protosB\017ElectionMessageb\006pr" +
      "oto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_Election_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_Election_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Election_descriptor,
        new java.lang.String[] { "SenderID", "WinnerID", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
