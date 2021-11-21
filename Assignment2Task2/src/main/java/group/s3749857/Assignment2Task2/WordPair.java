package group.s3749857.Assignment2Task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair> {

	private Text word;
	private Text neighbor;
	
	public WordPair() {
		this.word = new Text();
		this.neighbor = new Text();
	}
	
	public WordPair(String word, String neighbor) {
		this.word = new Text(word);
		this.neighbor = new Text(neighbor);
	}
	
	public WordPair(Text word, Text neighbor) {
		this.word = word;
		this.neighbor = neighbor;
	}
	
	public void setWord(String word) {
		this.word.set(word);
	}
	
	public void setNeighbor(String neighbor) {
		this.neighbor.set(neighbor);
	}
	
	public Text getWord() {
		return word;
	}
	
	public Text getNeighbor() {
		return neighbor;
	}
	
	/**
	 * The method of deserialization of the word and neighbor.
	 * The sequence of deserialization should be exactly the same as that of serialization.
	 */
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		neighbor.readFields(in);
	}

	/**
	 * The method of serialization of the word and neighbor.
	 */
	public void write(DataOutput out) throws IOException {
		word.write(out);
		neighbor.write(out);
	}
	
	/**
	 * The method will be applied when the process of sorting starts.
	 * @return Negative value like -1 represents that the current WordPair precedes the WordPair compared.
	 * @return 0 represents that the current WordPair is same as the WordPair compared.
	 * @return Positive value like 1 represents that the WordPair compared precedes the current WordPair.
	 */
	public int compareTo(WordPair o) {
		int compareVal = this.word.compareTo(o.getWord());
		// Sort the WordPair by the ASCII code of the first word if they are not same
		if (compareVal != 0) {
			return compareVal;
		}
		// Return 0 if their attributes are all same
		if (this.neighbor.compareTo(o.getNeighbor()) == 0) {
			return 0;
		}
		// The WordPair whose second string is '*' will come first among the WordPair with the same first word
		if (this.neighbor.toString().equals("*")) {
			return -1;
		}
		if (o.getNeighbor().toString().equals("*")) {
			return 1;
		}
		// Compare the second word if their first words are same and second string is not '*'
		return this.neighbor.compareTo(o.getNeighbor());
	}
	
	/**
	 * @return The format of the output from reducer.
	 */
	@Override
	public String toString() {
		return "(" + word.toString() + "," + neighbor.toString() + ")";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		WordPair other = (WordPair)obj;
		if (word == null) 
			if (other.word != null) return false;
		else if (!word.equals(other.word)) return false;
		if (neighbor == null) 
			if (other.neighbor != null) return false;
		else if (!neighbor.equals(other.neighbor)) return false;
		return true;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + (word == null ? 0 : word.hashCode());
		result = 31 * result + (neighbor == null ? 0 : neighbor.hashCode());
		return result;
	}
}
