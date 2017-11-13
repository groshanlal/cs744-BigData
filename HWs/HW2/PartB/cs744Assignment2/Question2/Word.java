package storm.starter.cs744Assignment2.Question2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Word implements Serializable, Comparable<Word> {
	String _word;
	int _count;
	public Word(String word) {
	    _word = word;
	    _count = 1;	    
	}
	public void incrementCount() {
	    ++_count;
	}
	public int getCount() {
	    return _count;
	}
	public String getWord() {
	    return _word;
	}
	@Override
	public int compareTo(Word other) {
	    return other.getCount() - this._count;	    
	}
}