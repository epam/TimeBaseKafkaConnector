package deltix.kafka.connect;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class FieldSelection {
    private Set<String> includedFields;
    private Set<String> excludedFields;

    public FieldSelection(String includedFields, String excludedFields) {
        this.includedFields = toSet(includedFields);
        this.excludedFields = toSet(excludedFields);

        if (this.includedFields != null && this.excludedFields != null && !Collections.disjoint(this.includedFields, this.excludedFields)) {
            throw new IllegalArgumentException("Included and excluded field lists contain common elements");
        }
    }

    private static Set<String> toSet(String valueList) {
        if (valueList == null || valueList.isEmpty())
            return null;

        Set<String> valueSet = new HashSet<>();
        for (String value : valueList.split(",")) {
            valueSet.add(value.trim());
        }
        return Collections.unmodifiableSet(valueSet);
    }

    public boolean isSelected(String fieldName) {
        return ((includedFields == null || includedFields.contains(fieldName)) &&
                (excludedFields == null || !excludedFields.contains(fieldName)));
    }

    public Set<String> getIncludedFields() {
        return includedFields;
    }

    public Set<String> getExcludedFields() {
        return excludedFields;
    }

    public boolean hasIncludedFields() {
        return includedFields != null;
    }

    public boolean hasExcludedFields() {
        return excludedFields != null;
    }
}
