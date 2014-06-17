package nl.naward04.wordtree;

public class KeyValuePair<K, V> {

	private K key;
	private V value;
	
	
	public KeyValuePair(K key, V value) {
		this.key = key;
		this.value = value;
	}
	
	public K getKey(){
		return key;
	}
	
	public V getValue() {
		return value;
	}
	
	@Override
	public boolean equals(Object o){
		if(!(o instanceof KeyValuePair)) {
			return false;
		} else if (this == o) {
			return true;
		}
		KeyValuePair object = (KeyValuePair)o;
		return this.key.equals(object.getKey());
	}
	
	public boolean hasKey(K key){
		return this.key.equals(key);
	}
	
	@Override
	public String toString() {
		return key.toString();
	}
	
}
