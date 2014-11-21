package edu.bigtextformat.levels;

import edu.bigtextformat.record.FormatType;
import edu.bigtextformat.record.FormatTypes;
import edu.bigtextformat.record.RecordFormat;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

public class LevelTest {

	private static final boolean LOAD = true;

	public static void main(String[] args) throws Exception {

		// List<Integer> toAdd = Arrays.asList(new Integer[] { 5, 91, 13, 72,
		// 53,
		// 33, 29, 94, 47, 80, 74, 44, 37, 68, 88, 62, 82, 39, 22, 48, 70,
		// 19, 34, 93, 99, 90, 11, 4, 54, 55, 98, 0, 86, 31, 79, 46, 76,
		// 87, 67, 3, 57, 9, 26, 89, 21, 14, 23, 20, 83, 51, 24, 10, 97,
		// 42, 50, 52, 25, 16, 73, 49, 30, 38, 92, 58, 71, 61, 36, 66, 84,
		// 8, 27, 43, 41, 85, 96, 60, 65, 63, 45, 77, 35, 6, 78, 59, 64,
		// 7, 2, 56, 18, 40, 32, 75, 17, 1, 15, 81, 12, 95, 69, 28 });
		// List<Integer> toAdd = Arrays.asList(new Integer[] { 0, 1, 3, 4, 10,
		// 12,
		// 13, 14, 2, 5, 6, 11, 8, 9, 20, 22, 50, 67, 12 });

		// int[] testSet = new int[] { 298, 193, 457, 690, 780, 892, 919, 71,
		// 970,
		// 433, 649, 96, 171, 29, 607, 372, 378, 603, 61, 335, 388, 42,
		// 19, 79, 414, 668, 357, 98, 402, 260, 691, 962, 157, 985, 40,
		// 489, 647, 192, 456, 918, 500, 449, 102, 228, 808, 8, 342, 182,
		// 720, 884, 487, 294, 475, 320, 760, 871, 698, 304, 912, 134,
		// 905, 170, 33, 638, 645, 606, 751, 947, 635, 704, 49, 309, 48,
		// 951, 579, 979, 915, 181, 777, 667, 117, 900, 445, 441, 349,
		// 448, 140, 999, 156, 271, 391, 27, 859, 353, 553, 488, 982, 101,
		// 870, 832, 901, 115, 289, 862, 162, 983, 183, 593, 844, 988,
		// 120, 253, 50, 584, 432, 547, 396, 429, 744, 721, 837, 282, 980,
		// 3, 995, 177, 920, 22, 518, 461, 534, 853, 209, 701, 511, 330,
		// 293, 944, 373, 754, 895, 663, 355, 595, 865, 424, 736, 334,
		// 225, 199, 612, 814, 128, 937, 533, 142, 9, 471, 578, 672, 932,
		// 421, 51, 787, 326, 597, 109, 718, 665, 248, 549, 483, 974, 75,
		// 245, 129, 885, 634, 149, 152, 745, 896, 822, 203, 6, 722, 159,
		// 377, 742, 1, 490, 210, 216, 627, 622, 971, 17, 986, 436, 35,
		// 223, 306, 779, 523, 165, 652, 679, 395, 280, 545, 143, 571,
		// 765, 809, 327, 13, 933, 232, 697, 470, 846, 978, 621, 705, 636,
		// 816, 65, 255, 153, 440, 491, 794, 964, 161, 576, 955, 662, 205,
		// 686, 303, 827, 344, 633, 233, 848, 197, 793, 975, 847, 864,
		// 759, 383, 460, 776, 772, 290, 790, 841, 464, 749, 531, 74, 564,
		// 700, 55, 12, 515, 546, 468, 273, 671, 604, 485, 317, 268, 688,
		// 406, 738, 111, 685, 24, 310, 874, 922, 52, 250, 570, 217, 548,
		// 479, 226, 637, 110, 981, 41, 385, 782, 830, 577, 928, 70, 130,
		// 107, 601, 583, 264, 513, 450, 21, 419, 567, 339, 632, 444, 888,
		// 218, 259, 405, 438, 240, 447, 867, 176, 714, 274, 370, 820,
		// 369, 64, 84, 270, 696, 613, 392, 453, 907, 114, 379, 786, 941,
		// 389, 543, 756, 757, 425, 681, 882, 554, 454, 778, 866, 572, 7,
		// 727, 618, 403, 542, 341, 494, 886, 329, 728, 45, 135, 659, 347,
		// 187, 190, 401, 302, 997, 39, 231, 60, 788, 127, 472, 300, 831,
		// 43, 133, 2, 103, 770, 230, 953, 166, 437, 825, 883, 845, 258,
		// 211, 969, 506, 413, 362, 95, 407, 917, 46, 639, 789, 375, 463,
		// 566, 719, 227, 541, 285, 286, 724, 699, 656, 706, 0, 992, 887,
		// 994, 972, 516, 773, 914, 58, 354, 556, 31, 367, 819, 191, 761,
		// 713, 416, 90, 352, 123, 426, 525, 869, 91, 852, 276, 557, 251,
		// 435, 658, 151, 654, 726, 20, 237, 154, 569, 945, 734, 949, 990,
		// 558, 537, 141, 446, 616, 507, 524, 57, 811, 803, 902, 750, 94,
		// 629, 459, 66, 828, 345, 791, 150, 562, 174, 333, 784, 891, 963,
		// 860, 796, 950, 683, 872, 247, 536, 380, 364, 168, 707, 806,
		// 138, 262, 480, 817, 561, 386, 509, 423, 146, 359, 173, 86, 628,
		// 443, 755, 292, 78, 818, 132, 261, 238, 644, 993, 591, 973, 673,
		// 269, 222, 676, 930, 702, 417, 418, 838, 368, 324, 959, 849,
		// 854, 568, 498, 911, 836, 899, 206, 219, 666, 296, 977, 560,
		// 723, 998, 677, 428, 956, 189, 642, 651, 252, 725, 275, 466,
		// 709, 38, 92, 966, 493, 602, 281, 180, 235, 801, 241, 641, 68,
		// 346, 528, 540, 758, 420, 308, 916, 582, 957, 137, 785, 387,
		// 898, 112, 752, 148, 692, 394, 617, 839, 646, 200, 799, 908,
		// 229, 943, 687, 674, 881, 439, 16, 336, 496, 929, 28, 484, 731,
		// 69, 476, 431, 530, 195, 741, 740, 473, 766, 481, 800, 224, 510,
		// 517, 868, 503, 753, 968, 954, 926, 147, 594, 890, 581, 600,
		// 321, 62, 716, 89, 462, 18, 215, 184, 961, 499, 851, 169, 119,
		// 284, 288, 37, 105, 213, 805, 478, 160, 735, 880, 295, 565, 277,
		// 202, 32, 97, 623, 834, 520, 648, 328, 856, 643, 323, 655, 823,
		// 279, 131, 544, 319, 640, 767, 842, 85, 53, 590, 585, 172, 798,
		// 664, 858, 620, 923, 196, 366, 795, 991, 314, 272, 781, 684, 83,
		// 598, 783, 63, 5, 927, 412, 826, 376, 178, 14, 682, 397, 212,
		// 44, 526, 879, 221, 650, 586, 381, 266, 356, 374, 124, 67, 532,
		// 680, 427, 4, 136, 693, 155, 508, 802, 25, 322, 404, 93, 952,
		// 434, 301, 774, 315, 630, 942, 325, 474, 711, 909, 236, 904,
		// 855, 482, 563, 492, 695, 708, 527, 340, 910, 121, 960, 411,
		// 311, 467, 958, 351, 875, 204, 125, 458, 894, 390, 521, 812,
		// 661, 313, 220, 861, 299, 739, 675, 657, 278, 244, 535, 559,
		// 940, 139, 11, 384, 422, 967, 243, 934, 730, 611, 186, 257, 331,
		// 946, 924, 316, 615, 104, 363, 631, 948, 239, 804, 495, 821,
		// 712, 906, 514, 399, 318, 939, 769, 234, 775, 358, 100, 987,
		// 283, 936, 550, 455, 207, 350, 876, 715, 188, 116, 10, 26, 194,
		// 185, 263, 409, 175, 163, 710, 732, 254, 797, 291, 361, 976,
		// 312, 106, 360, 519, 737, 913, 167, 73, 897, 935, 371, 486, 108,
		// 580, 589, 398, 249, 747, 451, 122, 609, 857, 771, 815, 522,
		// 703, 34, 614, 596, 670, 87, 198, 733, 746, 59, 214, 267, 810,
		// 164, 588, 77, 442, 626, 903, 729, 575, 689, 512, 653, 813, 382,
		// 23, 829, 82, 30, 573, 343, 768, 833, 889, 126, 824, 145, 763,
		// 415, 36, 365, 925, 539, 477, 497, 246, 54, 694, 47, 337, 574,
		// 762, 931, 88, 76, 599, 551, 208, 792, 80, 863, 624, 158, 625,
		// 893, 265, 179, 242, 99, 144, 410, 743, 843, 835, 592, 807, 256,
		// 538, 877, 748, 56, 984, 348, 430, 201, 873, 996, 669, 921, 505,
		// 552, 469, 529, 307, 608, 965, 465, 332, 587, 605, 502, 989,
		// 678, 297, 81, 619, 393, 305, 504, 878, 452, 287, 15, 850, 764,
		// 501, 840, 400, 717, 660, 338, 938, 610, 118, 408, 72, 555, 113 };

		// System.out.println(file.print());
		// System.out.println(toAdd);
		// Collections.sort(toAdd);
		// System.out.println(toAdd);
		String PATH = args[0];
		Integer SIZE = Integer.valueOf(args[1]);

		TIntArrayList toAdd = new TIntArrayList(SIZE);
		for (int i = 0; i < SIZE; i++)
			toAdd.add(i);
		// toAdd.addAll(testSet);

		// while (true) {
		// RecordFormat format = RecordFormat.create(new String[] { "k",
		// "k2" },
		// new FormatType<?>[] { FormatTypes.INTEGER.getType(),
		// FormatTypes.INTEGER.getType() }, new String[] { "k",
		// "k2" });

		// delete(new File(PATH));

		RecordFormat format = RecordFormat.create(new String[] { "k" },
				new FormatType<?>[] { FormatTypes.INTEGER.getType() },
				new String[] { "k" });
		SortedLevelFile file = SortedLevelFile.open(
				PATH,
				new LevelOptions().setFormat(format).setMaxMemTablesWriting(5)
						.setMemTableSize(32 * 1024 * 1024)
						.setBaseSize(2 * 1024 * 1024).setMaxLevel0Files(5)
						.setCompactLevel0Threshold(1).setMaxLevelFiles(20)
						.setMaxBlockSize(64 * 1024)
						.setCompressed(CompressionType.SNAPPY.getComp()));
		if (LOAD) {
			toAdd.shuffle(new Random(System.currentTimeMillis()));

			long init = System.currentTimeMillis();
			TIntIterator it = toAdd.iterator();
			int rawSize = 0;
			int cont = 0;
			while (it.hasNext()) {
				if (cont++ % 100000 == 0)
					System.out.println("Inserted " + cont);
				byte[] byteArray = format.newRecord().set("k", it.next())
				// .set("k2", -i)
						.toByteArray();
				byte[] bytes = "Hola!".getBytes();
				file.put(byteArray, bytes);

				rawSize += byteArray.length + bytes.length;
			}
			System.out.println("Raw Size " + rawSize);

			System.out.println(System.currentTimeMillis() - init);

			// System.out.println(file.print());

			System.out.println("Compacting...");
			long initCompact = System.currentTimeMillis();
			file.compact();
			System.out.println("Compact time: "
					+ (System.currentTimeMillis() - initCompact));

			// System.out.println(file.print());

			System.out.println("Closing...");
			file.close();

			System.out.println("Reopening...");
			file = SortedLevelFile.open(PATH, null);
		}
		System.out.println("Querying...");

		toAdd.sort();

		int contQuery = 0;

		long queryInit = System.currentTimeMillis();
		TIntIterator itQuery = toAdd.iterator();
		while (itQuery.hasNext()) {
			int integer = itQuery.next();
			if (contQuery % 100000 == 0)
				System.out.println("Current query count " + contQuery);
			if (!file.contains(DataTypeUtils.intToByteArray(integer))) {
				// System.out.println(toAdd);
				// System.out.println(file.print());
				throw new Exception("Not inserted! " + integer);
			}
			contQuery++;
		}
		System.out.println("Query Time "
				+ (System.currentTimeMillis() - queryInit));

		System.out.println("Closing...");
		file.close();
		// }

	}

	static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.isDirectory() && !f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}
}
