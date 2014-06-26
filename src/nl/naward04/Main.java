/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.naward04;

import java.util.Arrays;

import nl.naward04.hadoop.beverages.BeverageCounter;
import nl.naward04.hadoop.country.Country;
import nl.naward04.hadoop.wc.WordCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main entry point for the warcexamples. 
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class Main {
	
	public enum Programs {
		WORDCOUNT("wordcount", "Count the number of words in total"),
		COUNTRY("country","Find the server's country based on GeoIP"),
		BEVERAGES("beverage", "Find the Beverages per country"),
		;

		private final String name;
		private final String description;

		private Programs(String name, String description) {
			this.name = name;
			this.description = description;
		}

		public String getName() {
			return name;
		}

		public String getDescription() {
			return description;
		}
	}

	public static void main(String[] args) {
		int retval = 0;
		boolean showUsage = false;
		if(args.length <= 0) {
			showUsage();
			System.exit(0);
		}
		String tool = args[0];
		String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
		try {
			if (Programs.WORDCOUNT.getName().equals(tool)) {
				retval = ToolRunner.run(new Configuration(), new WordCounter(), toolArgs);
			} else if (Programs.COUNTRY.getName().equals(tool)) {
				retval = ToolRunner.run(new Configuration(), new Country(), toolArgs);
			} else if (Programs.BEVERAGES.getName().equals(tool)) {
				retval = ToolRunner.run(new Configuration(), new BeverageCounter(), toolArgs);
			}
			if (showUsage) {
				showUsage();
			}
		} catch (Exception e) {
			showErrorAndExit(e);
		}
		System.exit(retval);
	}

	private static void showErrorAndExit(Exception e) {
		System.out.println("Something didn't quite work like expected: [" + e.getMessage() + "]");
		showUsage();
		System.exit(1);
	}

	private static void showUsage() {
		System.out.println("An example program must be given as the first argument.");
		System.out.println("Valid program names are:");
		for (Programs prog : Programs.values()) {
			System.out.println(" " + prog.getName() + ": " + prog.getDescription());
		}
	}
}
