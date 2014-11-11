package mapreduce;

import java.io.Serializable;
import java.util.PriorityQueue;

public class Context<K extends Writable, V extends Writable> {

	private PriorityQueue<Entry> ans;

	private class Entry<Key extends Writable, Value extends Writable>
			implements Comparable {
		private Key key;
		private Value value;

		public Entry(Key key, Value value) {
			this.key = key;
			this.value = value;
		}

		public Key getKey() {
			return this.key;
		}

		public Value getValue() {
			return this.value;
		}

		@Override
		public int compareTo(Object o) {
			return this.key.hashCode() - ((Entry) o).getKey().hashCode();
		}
	}

	public void write(Text word, IntWritable one) {

	}
}
