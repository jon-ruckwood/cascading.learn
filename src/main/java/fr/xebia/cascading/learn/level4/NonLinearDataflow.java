package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s03.html
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {

		final Pipe presidents = new Pipe("presidents");
		final Pipe parties = new Pipe("parties");

		final Fields year = new Fields("year");
		final Fields declared = new Fields("year1", "president", "year2", "party");

		Pipe assembly = new CoGroup(presidents, year, parties, year, declared);
		assembly = new Retain(assembly, new Fields("president", "party"));

		return FlowDef.flowDef()
				.addSource(presidents, presidentsSource)
				.addSource(parties, partiesSource)
				.addTailSink(assembly, sink);
	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s07.html
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {

		final Pipe assembly = new Pipe("split");

		final Pipe gaullist = new Each(new Pipe("gaul", assembly),
				new Fields("party"),
				new ExpressionFilter("!\"Gaullist\".equals(party)", String.class));

		final Pipe republican = new Each(new Pipe("rep", assembly),
				new ExpressionFilter("!\"Republican\".equals(party)", String.class));

		final Pipe socialist = new Each(new Pipe("soc", assembly),
				new ExpressionFilter("!\"Socialist\".equals(party)", String.class));

		return FlowDef.flowDef()
				.addSource(assembly, source)
				.addTailSink(gaullist, gaullistSink)
				.addTailSink(republican, republicanSink)
				.addTailSink(socialist, socialistSink);
	}
}
