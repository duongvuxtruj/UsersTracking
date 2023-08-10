package kafka;

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class PhoneGenerator {
    private final Random rand = new Random();

    public Set<String> generateRandomPhones(int numberOfPhones) {
        Set<String> phoneNumbers = new HashSet<>();
        DecimalFormat df3 = new DecimalFormat("000"); // 3 zeros
        DecimalFormat df4 = new DecimalFormat("0000"); // 4 zeros

        while (phoneNumbers.size() < numberOfPhones) {
            int num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8);
            int num2 = rand.nextInt(743);
            int num3 = rand.nextInt(10000);

            String phoneNumber = df3.format(num1) + "-" + df3.format(num2) + "-" + df4.format(num3);
            phoneNumbers.add(phoneNumber);
        }

        return phoneNumbers;
    }

    public static void main(String[] args) {
        PhoneGenerator phoneGenerator = new PhoneGenerator();
        Set<String> randomPhones = phoneGenerator.generateRandomPhones(1000);

        for (String phoneNumber : randomPhones) {
            System.out.println(phoneNumber);
        }
    }
}

