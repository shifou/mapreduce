package data;

public class Message {
	public int id;
	msgType tp;
	public Message(int idd, msgType tpp)
	{
		tp=tpp;
		id=idd;
	}
	public Message(msgType tpp)
	{
		tp=tpp;
	}
}
