package com.appanddata.alsme.engine;

import java.util.Objects;

public class MemtableValue {
    private final String value;
    /**
     * To insert deleted key
     */
    private boolean tombstone;

    public MemtableValue(String value, boolean tombstone) {
        this.value = value;
        this.tombstone = tombstone;
    }

    public MemtableValue(String value) {
        this.value = value;
    }

    public static MemtableValue getDeletedValue() {
        return new MemtableValue("", true);
    }

    public String getValue() {
        return value;
    }

    public boolean isTombstone() {
        return tombstone;
    }


    public int length() {
        return 1 + value.length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemtableValue that = (MemtableValue) o;
        return tombstone == that.tombstone &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, tombstone);
    }
}
