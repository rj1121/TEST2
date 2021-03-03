package com.qf.bigdata.realtime.flink.util;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class QParameterTool implements Serializable, Cloneable{

    private static final long serialVersionUID = 1L;

    protected static final String NO_VALUE_KEY = "__NO_VALUE_KEY";
    protected static final String DEFAULT_UNDEFINED = "<undefined>";

    // ------------------ Constructors ------------------------

    /**
     * Returns {@link QParameterTool} for the given arguments. The arguments are keys followed by values.
     * Keys have to start with '-' or '--'
     *
     * <p><strong>Example arguments:</strong>
     * --key1 value1 --key2 value2 -key3 value3
     *
     * @param args Input array arguments
     * @return A {@link QParameterTool}
     */
    public static QParameterTool fromArgs(String[] args) {
        final Map<String, String> map = new HashMap<>(args.length / 2);

        int i = 0;
        while (i < args.length) {
            final String key;

            if (args[i].startsWith("--")) {
                key = args[i].substring(2);
            } else if (args[i].startsWith("-")) {
                key = args[i].substring(1);
            } else {
                throw new IllegalArgumentException(
                        String.format("Error parsing arguments '%s' on '%s'. Please prefix keys with -- or -.",
                                Arrays.toString(args), args[i]));
            }

            if (key.isEmpty()) {
                throw new IllegalArgumentException(
                        "The input " + Arrays.toString(args) + " contains an empty argument");
            }

            i += 1; // try to find the value

            if (i >= args.length) {
                map.put(key, NO_VALUE_KEY);
            } else if (NumberUtils.isNumber(args[i])) {
                map.put(key, args[i]);
                i += 1;
            } else if (args[i].startsWith("--") || args[i].startsWith("-")) {
                // the argument cannot be a negative number because we checked earlier
                // -> the next argument is a parameter name
                map.put(key, NO_VALUE_KEY);
            } else {
                map.put(key, args[i]);
                i += 1;
            }
        }

        return fromMap(map);
    }

    /**
     * Returns {@link QParameterTool} for the given {@link Properties} file.
     *
     * @param path Path to the properties file
     * @return A {@link QParameterTool}
     * @throws IOException If the file does not exist
     * @see Properties
     */
    public static QParameterTool fromPropertiesFile(String path) throws IOException {
        File propertiesFile = new File(path);
        return fromPropertiesFile(propertiesFile);
    }

    /**
     * Returns {@link QParameterTool} for the given {@link Properties} file.
     *
     * @param file File object to the properties file
     * @return A {@link QParameterTool}
     * @throws IOException If the file does not exist
     * @see Properties
     */
    public static QParameterTool fromPropertiesFile(File file) throws IOException {
        if (!file.exists()) {
            throw new FileNotFoundException("Properties file " + file.getAbsolutePath() + " does not exist");
        }
        try (FileInputStream fis = new FileInputStream(file)) {
            return fromPropertiesFile(fis);
        }
    }

    /**
     * Returns {@link QParameterTool} for the given InputStream from {@link Properties} file.
     *
     * @param inputStream InputStream from the properties file
     * @return A {@link QParameterTool}
     * @throws IOException If the file does not exist
     * @see Properties
     */
    public static QParameterTool fromPropertiesFile(InputStream inputStream) throws IOException {
        Properties props = new Properties();
        props.load(inputStream);
        return fromMap((Map) props);
    }

    /**
     * Returns {@link QParameterTool} for the given map.
     *
     * @param map A map of arguments. Both Key and Value have to be Strings
     * @return A {@link QParameterTool}
     */
    public static QParameterTool fromMap(Map<String, String> map) {
        return new QParameterTool(map);
    }

    /**
     * Returns {@link QParameterTool} from the system properties.
     * Example on how to pass system properties:
     * -Dkey1=value1 -Dkey2=value2
     *
     * @return A {@link QParameterTool}
     */
    public static QParameterTool fromSystemProperties() {
        return fromMap((Map) System.getProperties());
    }

    // ------------------ QParameterTool  ------------------------
    public  Map<String, String> data = null;

    // data which is only used on the client and does not need to be transmitted
    protected transient Map<String, String> defaultData;
    protected transient Set<String> unrequestedParameters;

    private QParameterTool(Map<String, String> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));

        this.defaultData = new ConcurrentHashMap<>(data.size());

        this.unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));

        unrequestedParameters.addAll(data.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QParameterTool that = (QParameterTool) o;
        return Objects.equals(data, that.data) &&
                Objects.equals(defaultData, that.defaultData) &&
                Objects.equals(unrequestedParameters, that.unrequestedParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, defaultData, unrequestedParameters);
    }

    /**
     * Returns the set of parameter names which have not been requested with
     * {@link #has(String)} or one of the {@code get} methods. Access to the
     * map returned by {@link #toMap()} is not tracked.
     */
    public Set<String> getUnrequestedParameters() {
        return Collections.unmodifiableSet(unrequestedParameters);
    }

    // ------------------ Get data from the util ----------------

    /**
     * Returns number of parameters in {@link QParameterTool}.
     */
    public int getNumberOfParameters() {
        return data.size();
    }

    /**
     * Returns the String value for the given key.
     * If the key does not exist it will return null.
     */
    public String get(String key) {
        addToDefaults(key, null);
        unrequestedParameters.remove(key);
        return data.get(key);
    }

    /**
     * Returns the String value for the given key.
     * If the key does not exist it will throw a {@link RuntimeException}.
     */
    public String getRequired(String key) {
        addToDefaults(key, null);
        String value = get(key);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        }
        return value;
    }

    /**
     * Returns the String value for the given key.
     * If the key does not exist it will return the given default value.
     */
    public String get(String key, String defaultValue) {
        addToDefaults(key, defaultValue);
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return value;
        }
    }

    /**
     * Check if value is set.
     */
    public boolean has(String value) {
        addToDefaults(value, null);
        unrequestedParameters.remove(value);
        return data.containsKey(value);
    }

    // -------------- Integer

    /**
     * Returns the Integer value for the given key.
     * The method fails if the key does not exist or the value is not an Integer.
     */
    public int getInt(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Integer.parseInt(value);
    }

    /**
     * Returns the Integer value for the given key. If the key does not exists it will return the default value given.
     * The method fails if the value is not an Integer.
     */
    public int getInt(String key, int defaultValue) {
        addToDefaults(key, Integer.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    // -------------- LONG

    /**
     * Returns the Long value for the given key.
     * The method fails if the key does not exist.
     */
    public long getLong(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Long.parseLong(value);
    }

    /**
     * Returns the Long value for the given key. If the key does not exists it will return the default value given.
     * The method fails if the value is not a Long.
     */
    public long getLong(String key, long defaultValue) {
        addToDefaults(key, Long.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    // -------------- FLOAT

    /**
     * Returns the Float value for the given key.
     * The method fails if the key does not exist.
     */
    public float getFloat(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Float.valueOf(value);
    }

    /**
     * Returns the Float value for the given key. If the key does not exists it will return the default value given.
     * The method fails if the value is not a Float.
     */
    public float getFloat(String key, float defaultValue) {
        addToDefaults(key, Float.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Float.valueOf(value);
        }
    }

    // -------------- DOUBLE

    /**
     * Returns the Double value for the given key.
     * The method fails if the key does not exist.
     */
    public double getDouble(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Double.valueOf(value);
    }

    /**
     * Returns the Double value for the given key. If the key does not exists it will return the default value given.
     * The method fails if the value is not a Double.
     */
    public double getDouble(String key, double defaultValue) {
        addToDefaults(key, Double.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Double.valueOf(value);
        }
    }

    // -------------- BOOLEAN

    /**
     * Returns the Boolean value for the given key.
     * The method fails if the key does not exist.
     */
    public boolean getBoolean(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Boolean.valueOf(value);
    }

    /**
     * Returns the Boolean value for the given key. If the key does not exists it will return the default value given.
     * The method returns whether the string of the value is "true" ignoring cases.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        addToDefaults(key, Boolean.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Boolean.valueOf(value);
        }
    }

    // -------------- SHORT

    /**
     * Returns the Short value for the given key.
     * The method fails if the key does not exist.
     */
    public short getShort(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Short.valueOf(value);
    }

    /**
     * Returns the Short value for the given key. If the key does not exists it will return the default value given.
     * The method fails if the value is not a Short.
     */
    public short getShort(String key, short defaultValue) {
        addToDefaults(key, Short.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Short.valueOf(value);
        }
    }

    // -------------- BYTE

    /**
     * Returns the Byte value for the given key.
     * The method fails if the key does not exist.
     */
    public byte getByte(String key) {
        addToDefaults(key, null);
        String value = getRequired(key);
        return Byte.valueOf(value);
    }

    /**
     * Returns the Byte value for the given key. If the key does not exists it will return the default value given.
     * The method fails if the value is not a Byte.
     */
    public byte getByte(String key, byte defaultValue) {
        addToDefaults(key, Byte.toString(defaultValue));
        String value = get(key);
        if (value == null) {
            return defaultValue;
        } else {
            return Byte.valueOf(value);
        }
    }

    // --------------- Internals

    protected void addToDefaults(String key, String value) {
        final String currentValue = defaultData.get(key);
        if (currentValue == null) {
            if (value == null) {
                value = DEFAULT_UNDEFINED;
            }
            defaultData.put(key, value);
        } else {
            // there is already an entry for this key. Check if the value is the undefined
            if (currentValue.equals(DEFAULT_UNDEFINED) && value != null) {
                // update key with better default value
                defaultData.put(key, value);
            }
        }
    }

    // ------------------------- Export to different targets -------------------------


    /**
     * @return A {@link Properties}
     */
    public Properties getProperties() {
        Properties props = new Properties();
        props.putAll(this.data);
        return props;
    }

    /**
     * Create a properties file with all the known parameters (call after the last get*() call).
     * Set the default value, if available.
     *
     * <p>Use this method to create a properties file skeleton.
     *
     * @param pathToFile Location of the default properties file.
     */
    public void createPropertiesFile(String pathToFile) throws IOException {
        createPropertiesFile(pathToFile, true);
    }

    /**
     * Create a properties file with all the known parameters (call after the last get*() call).
     * Set the default value, if overwrite is true.
     *
     * @param pathToFile Location of the default properties file.
     * @param overwrite Boolean flag indicating whether or not to overwrite the file
     * @throws IOException If overwrite is not allowed and the file exists
     */
    public void createPropertiesFile(String pathToFile, boolean overwrite) throws IOException {
        final File file = new File(pathToFile);
        if (file.exists()) {
            if (overwrite) {
                file.delete();
            } else {
                throw new RuntimeException("File " + pathToFile + " exists and overwriting is not allowed");
            }
        }
        final Properties defaultProps = new Properties();
        defaultProps.putAll(this.defaultData);
        try (final OutputStream out = new FileOutputStream(file)) {
            defaultProps.store(out, "Default file created by Flink's ParameterUtil.createPropertiesFile()");
        }
    }

    protected Object clone() throws CloneNotSupportedException {
        return new QParameterTool(this.data);
    }

    // ------------------------- Interaction with other ParameterUtils -------------------------

    /**
     * Merges two {@link QParameterTool}.
     *
     * @param other Other {@link QParameterTool} object
     * @return The Merged {@link QParameterTool}
     */
    public QParameterTool mergeWith(QParameterTool other) {
        final Map<String, String> resultData = new HashMap<>(data.size() + other.data.size());
        resultData.putAll(data);
        resultData.putAll(other.data);

        final QParameterTool ret = new QParameterTool(resultData);

        final HashSet<String> requestedParametersLeft = new HashSet<>(data.keySet());
        requestedParametersLeft.removeAll(unrequestedParameters);

        final HashSet<String> requestedParametersRight = new HashSet<>(other.data.keySet());
        requestedParametersRight.removeAll(other.unrequestedParameters);

        ret.unrequestedParameters.removeAll(requestedParametersLeft);
        ret.unrequestedParameters.removeAll(requestedParametersRight);

        return ret;
    }

    // ------------------------- ExecutionConfig.UserConfig interface -------------------------

    public Map<String, String> toMap() {
        return data;
    }

    // ------------------------- Serialization ---------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        defaultData = new ConcurrentHashMap<>(data.size());
        unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap<>(data.size()));
    }


}
