package com.broaddata.common.util;

import java.io.Reader;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;

public class TermVectorTextField extends Field {

	/* Indexed, tokenized, not stored. */
	public static final FieldType TYPE_WITH_TERM_VECTOR = new FieldType();

	static {
		//TYPE_WITH_TERM_VECTOR.setIndexed(true);
		TYPE_WITH_TERM_VECTOR.setTokenized(true);
		TYPE_WITH_TERM_VECTOR.setStoreTermVectors(true);
		TYPE_WITH_TERM_VECTOR.freeze();
	}

	public TermVectorTextField(String name, Reader reader) {
		super(name, reader, TYPE_WITH_TERM_VECTOR);
	}
        
	public TermVectorTextField(String name,String value) {
		super(name, value, TYPE_WITH_TERM_VECTOR);
	}
        
}