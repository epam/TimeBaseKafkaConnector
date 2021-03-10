package deltix.kafka.connect;

import deltix.qsrv.hf.pub.InstrumentType;
import deltix.qsrv.hf.pub.RawMessage;
import deltix.qsrv.hf.pub.WritableValue;
import deltix.qsrv.hf.pub.codec.FixedUnboundEncoder;
import deltix.qsrv.hf.pub.codec.InterpretingCodecMetaFactory;
import deltix.qsrv.hf.pub.codec.UnboundEncoder;
import deltix.qsrv.hf.pub.md.*;
import deltix.util.memory.MemoryDataOutput;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static deltix.kafka.connect.LogicalType.*;
import static deltix.kafka.connect.TBConnectorConfig.NO_SYMBOL;
import static deltix.qsrv.hf.pub.md.BinaryDataType.MIN_COMPRESSION;
import static deltix.qsrv.hf.pub.md.FloatDataType.ENCODING_FIXED_DOUBLE;
import static deltix.qsrv.hf.pub.md.FloatDataType.ENCODING_FIXED_FLOAT;
import static deltix.qsrv.hf.pub.md.IntegerDataType.*;
import static deltix.qsrv.hf.pub.md.VarcharDataType.ENCODING_INLINE_VARSIZE;

public class RawMessageSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(RawMessageSerializer.class);

    private final Map<String, FixedUnboundEncoder> encoders = new HashMap<>();
    private final MemoryDataOutput output = new MemoryDataOutput();

    private List<String> flattenFields;
    private String tbOffsetField;
    private String tbKeyField;
    private String tbTombstoneField;
    private String instrumentField;
    private String symbolField;
    private String timeField;
    private FieldSelection fieldSelection;
    private Set<String> timestampFields;
    private FieldMap fieldMap;
    private RecordClassDescriptor descriptor;

    public RawMessageSerializer(TBConnectorConfig config, RecordClassDescriptor descriptor, Schema schema) {
        this.flattenFields = config.getFlattenFields();
        this.tbOffsetField = config.getTBMessageIDField();
        this.tbKeyField = config.getTBMessageKeyField();
        this.tbTombstoneField = config.getTBMessageTombstoneField();
        this.instrumentField = config.getInstrumentField();
        this.symbolField = config.getSymbolField();
        this.timeField = config.getTimeField();
        this.fieldSelection = config.getFieldSelection();
        this.timestampFields = config.getTimestampFields();
        this.fieldMap = config.getNameAliases();

        if (this.tbTombstoneField != null && this.tbKeyField == null) {
            throw new IllegalArgumentException("TimeBase key field is required when storing tombstone records");
        }

        this.descriptor = descriptor;
        if (this.descriptor != null) {
            if (schema != null)
                validateSchema(schema);
            if (tbOffsetField != null && !descriptor.hasField(tbOffsetField))
                throw new IllegalArgumentException("TimeBase stream descriptor is missing configured offset field: " + tbOffsetField);
            if (tbKeyField != null && !descriptor.hasField(tbKeyField))
                throw new IllegalArgumentException("TimeBase stream descriptor is missing configured key field: " + tbKeyField);
            if (tbTombstoneField != null && !descriptor.hasField(tbTombstoneField))
                throw new IllegalArgumentException("TimeBase stream descriptor is missing configured tombstone field: " + tbTombstoneField);
            LOG.info("Using existing TimeBase stream descriptor: " + descriptor);
        } else {
            this.descriptor = buildDescriptor(schema);
            LOG.info("Built TimeBase stream descriptor for " + schema.name() + ": " + this.descriptor);
        }
    }

    public RawMessage serialize(SinkRecord record, long lastMessageTimeStamp) {
        Struct value = (Struct) record.value();
        if (value == null && (tbTombstoneField == null || descriptor == null)) {
            LOG.warn("Ignoring tombstone record with the key " + record.key());
            return null;
        }
        else if (descriptor == null) {
            descriptor = buildDescriptor(record.valueSchema());
            LOG.info("Built Descriptor for " + record.valueSchema().name() + ": " + descriptor);
        }

        FixedUnboundEncoder encoder = getEncoder(descriptor);

        output.reset();
        encoder.beginWrite(output);
        writeStruct(value, record, descriptor, encoder);
        encoder.endWrite();

        RawMessage message = new RawMessage(descriptor);
        message.setInstrumentType(instrumentField == null || value == null ? InstrumentType.CUSTOM : InstrumentType.valueOf(value.getString(instrumentField)));
        message.setSymbol(symbolField == null || value == null ? NO_SYMBOL : value.getString(symbolField));
        Long timestamp = timeField == null  ? record.timestamp() : value == null ? lastMessageTimeStamp : getTimestamp(value, timeField);
        if (timestamp != null)
            message.setTimeStampMs(timestamp);

        message.copyBytes(output, 0);
        return message;
    }

    private Long getTimestamp(Struct value, String fieldName) {
        Field timeField = value.schema().field(fieldName);
        if (timeField == null)
            throw new IllegalArgumentException("Schema has no time field " + fieldName);
        return toInt64(value.get(timeField), timeField.schema());
    }

    private void writeNull(UnboundEncoder encoder, DataType dataType) {
        if (dataType.isNullable()) {
            encoder.writeNull();
        }
        else if (dataType instanceof VarcharDataType) {
            encoder.writeString("");
        }
        else if (dataType instanceof BooleanDataType) {
            encoder.writeBoolean(false);
        }
        else if (dataType instanceof IntegerDataType) {
            if (((IntegerDataType) dataType).getNativeTypeSize() > 4)
                encoder.writeLong(0);
            else
                encoder.writeInt(0);
        }
        else if (dataType instanceof FloatDataType) {
            if (((FloatDataType) dataType).isFloat())
                encoder.writeFloat(0);
            else
                encoder.writeDouble(0);
        }
        else if (dataType instanceof BinaryDataType) {
            encoder.writeBinary(new byte[] { 0 }, 0, 1);
        }
        else if (dataType instanceof ArrayDataType) {
            encoder.setArrayLength(0);
        }
        else if (dataType instanceof TimeOfDayDataType) {
            encoder.writeInt(0);
        }
        else if (dataType instanceof DateTimeDataType) {
            encoder.writeLong(0);
        }
        else if (dataType instanceof ClassDataType) {
            RecordClassDescriptor fieldDescriptor = ((ClassDataType) dataType).getFixedDescriptor();
            UnboundEncoder fieldEncoder = encoder.getFieldEncoder(fieldDescriptor);
            for (DataField dataField : fieldDescriptor.getFields()) {
                if (!fieldEncoder.nextField()) {
                    throw new RuntimeException("Encoder does not match descriptor at field " + dataField.getName());
                }
                writeNull(fieldEncoder, dataField.getType());
            }
        }
        else if (dataType instanceof CharDataType) {
            encoder.writeChar(' ');
        }
        else { //we do not create EnumDataType, QueryDataType
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private void writeStruct(Struct value, SinkRecord record, RecordClassDescriptor descriptor, UnboundEncoder encoder) {

        for (DataField dataField : descriptor.getFields()) {
            if (! encoder.nextField()) {
                throw new RuntimeException("Encoder does not match descriptor at field " + dataField.getName());
            }

            String fieldName = dataField.getName();
            if (record != null) {
                if (tbOffsetField != null && fieldName.equals(tbOffsetField)) {
                    encoder.writeLong(record.kafkaOffset());
                    continue;
                }
                else if (tbKeyField != null && fieldName.equals(tbKeyField)) {
                    // expecting String keys
                    encoder.writeString(String.valueOf(record.key()));
                    continue;
                }
                else if (tbTombstoneField != null && fieldName.equals(tbTombstoneField)) {
                    encoder.writeBoolean(value == null);
                    continue;
                }
            }

            if (value == null) {
                // this is tombstone record
                writeNull(encoder, dataField.getType());
            }
            else {
                String[] sourcePath = fieldMap.getSourcePath(fieldName);
                Field field = getField(value.schema(), sourcePath);
                Object fieldValue = getFieldValue(value, sourcePath);

                if (fieldValue == null) {
                    //LOG.debug("Writing null for " + field.name() + ": " + field.schema().type() + ": " + dataField.getType());
                    encoder.writeNull();
                } else {
                    //LOG.debug("Writing " + field.name() + ": " + field.schema().type() + ": " + dataField.getType());
                    switch (field.schema().type()) {
                        case INT8:
                            encoder.writeInt((Byte) fieldValue);
                            break;
                        case INT16:
                            encoder.writeInt((Short) fieldValue);
                            break;
                        case INT32:
                            encoder.writeInt(toInt32(fieldValue, field.schema()));
                            break;
                        case INT64:
                            encoder.writeLong(toInt64(fieldValue, field.schema()));
                            break;
                        case STRING:
                            encoder.writeString((String) fieldValue);
                            break;
                        case FLOAT32:
                            encoder.writeFloat((Float) fieldValue);
                            break;
                        case FLOAT64:
                            encoder.writeDouble((Double) fieldValue);
                            break;
                        case BOOLEAN:
                            encoder.writeBoolean((Boolean) fieldValue);
                            break;
                        case BYTES:
                            byte[] bytes = (fieldValue instanceof ByteBuffer) ? ((ByteBuffer) fieldValue).array() : (byte[]) fieldValue;
                            encoder.writeBinary(bytes, 0, bytes.length);
                            break;
                        case ARRAY:
                            DataType elementType = ((ArrayDataType) dataField.getType()).getElementDataType();
                            writeArray((List<?>) fieldValue, field.schema().valueSchema(), elementType, encoder);
                            break;
                        case STRUCT:
                            RecordClassDescriptor fieldDescriptor = ((ClassDataType) dataField.getType()).getFixedDescriptor();
                            UnboundEncoder fieldEncoder = encoder.getFieldEncoder(fieldDescriptor);
                            writeStruct((Struct) fieldValue, null, fieldDescriptor, fieldEncoder);
                            break;
                        case MAP:
                        default:
                            throw new IllegalArgumentException("Unsupported field type " + field.schema().type());
                    }
                }
            }
        }
    }

    private Field getField(Schema schema, String[] fieldPath) {
        Field field = schema.field(fieldPath[0]);
        for (int i = 1; field != null && i < fieldPath.length; i++) {
            field = field.schema().field(fieldPath[i]);
        }
        return field;
    }

    private Object getFieldValue(Struct value, String[] fieldPath) {
        Object fieldValue = value.get(fieldPath[0]);
        for (int i = 1; fieldValue != null && i < fieldPath.length; i++) {
            fieldValue = ((Struct) fieldValue).get(fieldPath[i]);
        }
        return fieldValue;
    }

    private int toInt32(Object fieldValue, Schema fieldSchema) {
        if (fieldValue instanceof Integer) {
            return (Integer) fieldValue;
        }
        else if (fieldValue instanceof Date) {
            LogicalType logicalType = LogicalType.getTypeByName(fieldSchema.name());
            if (logicalType == DATE) {
                // convert logicalType date to number of days
                return (int) (((Date) fieldValue).getTime() / TimeUnit.DAYS.toMillis(1));
            }
            else if (logicalType == TIME_MS) {
                return Time.fromLogical(fieldSchema, (Date) fieldValue);
            }
        }
        throw new RuntimeException("Unexpected INT32 field value type: " + fieldValue.getClass().getName());
    }

    private long toInt64(Object fieldValue, Schema fieldSchema) {
        if (fieldValue instanceof Long) {
            return (Long) fieldValue;
        }
        else if (fieldValue instanceof Date) {
            // convert based on logical type
            LogicalType logicalType = LogicalType.getTypeByName(fieldSchema.name());
            if (logicalType == TIMESTAMP_MS || logicalType == LOCAL_TIMESTAMP_MS)
                return ((Date) fieldValue).getTime();
        }
        throw new RuntimeException("Unexpected INT64 field value type: " + fieldValue.getClass().getName() + " for logical type: " + fieldSchema.name());
    }

    private void writeArray(List<?> arrayVal, Schema elementSchema, DataType elementType, WritableValue encoder) {
        encoder.setArrayLength(arrayVal.size());
        for (Object element : arrayVal) {
            WritableValue v = encoder.nextWritableElement();
            if (element == null) {
                v.writeNull();
            } else {
                switch (elementSchema.type()) {
                    case INT8:
                        v.writeInt((Byte) element);
                        break;
                    case INT16:
                        v.writeInt((Short) element);
                        break;
                    case INT32:
                        v.writeInt(toInt32(element, elementSchema));
                        break;
                    case INT64:
                        v.writeLong(toInt64(element, elementSchema));
                        break;
                    case FLOAT32:
                        v.writeFloat((Float) element);
                        break;
                    case FLOAT64:
                        v.writeDouble((Double) element);
                        break;
                    case BOOLEAN:
                        v.writeBoolean((Boolean) element);
                        break;
                    case STRING:
                        v.writeString((String) element);
                        break;
                    case BYTES:
                        byte[] bytes = (element instanceof ByteBuffer) ? ((ByteBuffer) element).array() : (byte[]) element;
                        v.writeBinary(bytes, 0, bytes.length);
                        break;
                    case ARRAY:
                        DataType nestedElementType = ((ArrayDataType) elementType).getElementDataType();
                        writeArray((List<?>) element, elementSchema.valueSchema(), nestedElementType, v);
                        break;
                    case STRUCT:
                        RecordClassDescriptor nestedDescriptor = ((ClassDataType) elementType).getFixedDescriptor();
                        writeStruct((Struct) element, null, nestedDescriptor, v.getFieldEncoder(nestedDescriptor));
                        break;
                    case MAP:
                    default:
                        throw new IllegalArgumentException("Unsupported array element type: " + elementType);
                }
            }
        }
    }

    private RecordClassDescriptor buildDescriptor(Schema schema) {
        validateSchema(schema);

        List<DataField> fields = new ArrayList<>();
        if (tbOffsetField != null) {
            DataField offsetDataField = new NonStaticDataField(tbOffsetField, tbOffsetField, new IntegerDataType(ENCODING_INT64, false));
            offsetDataField.setDescription("Kafka Record Offset");
            fields.add(offsetDataField);
        }

        if (tbKeyField != null) {
            DataField keyDataField = new NonStaticDataField(tbKeyField, tbKeyField, VarcharDataType.getDefaultInstance());
            keyDataField.setDescription("Kafka Record Key");
            fields.add(keyDataField);
        }

        if (tbTombstoneField != null) {
            DataField tombstoneField = new NonStaticDataField(tbTombstoneField, tbTombstoneField, BooleanDataType.getDefaultInstance());
            tombstoneField.setDescription("Is Kafka Tombstone Record");
            fields.add(tombstoneField);
        }

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            boolean flatten = flattenFields != null && flattenFields.contains(fieldName);
            addDataField(fieldName, field.schema(), fields, flatten);
        }

        return new RecordClassDescriptor(schema.name(), schema.name(), schema.doc(), false, null, fields.toArray(new DataField[0]));
    }

    private void addDataField(String fieldName, Schema fieldSchema, List<DataField> fields, boolean flatten) {

        if (flatten && fieldSchema.type() == Schema.Type.STRUCT) {
            for (Field field : fieldSchema.fields()) {
                addDataField(fieldMap.addToPath(fieldName, field.name()), field.schema(), fields, flatten);
            }
        }
        else {
            if (fieldSelection.isSelected(fieldName)) {
                DataType fieldType = getDataType(fieldName, fieldSchema);
                if (fieldType != null) {
                    fieldName = fieldMap.getDestination(fieldName); //rename
                    DataField dataField = new NonStaticDataField(fieldName, fieldName, fieldType);
                    dataField.setDescription(fieldSchema.doc());
                    fields.add(dataField);
                    if (LOG.isDebugEnabled())
                        LOG.debug("Added Field " + fieldName + "/" + fieldType.getClass());
                }
            }
            else {
                LOG.debug("Skipping Excluded Field " + fieldName);
            }
        }
    }

    private RecordClassDescriptor buildFieldDescriptor(Schema schema) {
        List<DataField> fields = new ArrayList<>();
        for (Field field : schema.fields()) {
            addDataField(field.name(), field.schema(), fields, false);
        }
        return new RecordClassDescriptor(schema.name(), schema.name(), schema.doc(), false, null, fields.toArray(new DataField[0]));
    }

    private void validateSchema(Schema schema) {
        if (instrumentField != null)
            validateSchemaField(schema, instrumentField, Schema.Type.STRING);
        if (symbolField != null)
            validateSchemaField(schema, symbolField, Schema.Type.STRING);
        if (timeField != null)
            validateSchemaField(schema, timeField, Schema.Type.INT64);

        // verify referenced source fields exist in the schema
        validateSourceFields(schema, fieldSelection.getExcludedFields());
        validateSourceFields(schema, fieldSelection.getIncludedFields());
        validateSourceFields(schema, fieldMap.getSourceFields());

        // check for conflicts with new TB fields
        validateNewFields(schema, tbOffsetField, tbKeyField, tbTombstoneField);
    }

    private void validateSourceFields(Schema schema, Set<String> srcFields) {
        if (srcFields != null && !srcFields.isEmpty()) {
            for (String srcField : srcFields) {
                String destField = fieldMap.getDestination(srcField);
                String[] srcPath = fieldMap.getSourcePath(destField);
                if (getField(schema, srcPath) == null) {
                    throw new IllegalArgumentException("Schema is missing specified field: " + srcField);
                }
            }
        }
    }

    private void validateNewFields(Schema schema, String... newFields) {
        for (int i = 0; i < newFields.length; i++) {
            String newField = newFields[i];
            if (newField != null) {
                validateNewField(schema, newField);

                //search for duplicates
                for (int j = i+1; j < newFields.length; j++) {
                    if (newField.equals(newFields[j]))
                        throw new IllegalArgumentException("\"" + newField + "\" name is configured for multiple fields");
                }
            }
        }
    }

    private void validateNewField(Schema schema, String newField) {
        if (fieldMap.hasDestinationAlias(newField))
            throw new IllegalArgumentException("Specified new field name \"" + newField + "\" is already used as a field alias");

        if (schema.field(newField) != null && !fieldMap.hasSourceAlias(newField))
            throw new IllegalArgumentException("Schema already contains specified new field: " + newField);

        if (fieldMap.isFieldPath(newField))
            throw new IllegalArgumentException("Specified new filed name \"" + newField + "\" contains path separator character");
    }

    private static void validateSchemaField(Schema schema, String fieldName, Schema.Type fieldType) {
        Field field = schema.field(fieldName);
        if (field == null) {
            throw new IllegalArgumentException("Schema has no field " + fieldName);
        }
        if (fieldType != null && field.schema().type() != fieldType) {
            throw new IllegalArgumentException("Field " + fieldName + " must be a " + fieldType);
        }
    }

    private DataType getDataType(String fieldName, Schema fieldSchema) {
        boolean nullable = fieldSchema.isOptional();

        switch (fieldSchema.type()) {
            case INT8:
                return new IntegerDataType(ENCODING_INT8, nullable);
            case INT16:
                return new IntegerDataType(ENCODING_INT16, nullable);
            case INT32:
                LogicalType logicalIntType = LogicalType.getTypeByName(fieldSchema.name());
                if (logicalIntType == TIME_MS) {
                    return new TimeOfDayDataType(nullable);
                }
                return new IntegerDataType(ENCODING_INT32, nullable);
            case INT64:
                LogicalType logicalLongType = LogicalType.getTypeByName(fieldSchema.name());
                if (logicalLongType == TIMESTAMP_MS || (timestampFields != null && timestampFields.contains(fieldName))) {
                    return new DateTimeDataType(nullable);
                }
                return new IntegerDataType(ENCODING_INT64, nullable);
            case STRING:
                return new VarcharDataType(ENCODING_INLINE_VARSIZE, nullable, false);
            case FLOAT32:
                return new FloatDataType(ENCODING_FIXED_FLOAT, nullable);
            case FLOAT64:
                return new FloatDataType(ENCODING_FIXED_DOUBLE, nullable);
            case BOOLEAN:
                return new BooleanDataType(nullable);
            case BYTES:
                return new BinaryDataType(nullable, MIN_COMPRESSION);
            case ARRAY:
                DataType elementType = getDataType(fieldName, fieldSchema.valueSchema());
                if (elementType == null || elementType instanceof VarcharDataType) {
                    LOG.warn("Ignoring Array of Strings field: " + fieldName);
                    return null; //TODO timebase allows arrays of alphanumeric varchar (length < 10)
                }
                return new ArrayDataType(nullable, elementType);
            case STRUCT:
                return new ClassDataType(nullable, buildFieldDescriptor(fieldSchema));
            case MAP:
            default:
                throw new IllegalArgumentException("Unsupported field type " + fieldSchema.type());
        }
    }

    private FixedUnboundEncoder getEncoder (final RecordClassDescriptor type) {
        String guid = type.getGuid();
        FixedUnboundEncoder encoder = encoders.get(guid);
        if (encoder == null) {
            encoder = InterpretingCodecMetaFactory.INSTANCE.createFixedUnboundEncoderFactory(type).create();
            encoders.put(guid, encoder);
        }
        return encoder;
    }
}
