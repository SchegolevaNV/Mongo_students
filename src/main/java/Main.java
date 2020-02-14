import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) throws IOException {

        String filePath = "src/main/data/mongo.csv";
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

        MongoDatabase database = mongoClient.getDatabase("local");
        MongoCollection<Document> collection = database.getCollection("Students");
        collection.drop();

        for (String line : lines) {
            Pattern pattern = Pattern.compile("(?<name>.+),(?<age>[0-9]+),(?<courses>.+)");
            Matcher matcher = pattern.matcher(line);
             if (matcher.find()) {
                 Document studentInfo = new Document()
                         .append("name", matcher.group("name"))
                         .append("age", Integer.parseInt(matcher.group("age")))
                         .append("courses", matcher.group("courses"));
                 collection.insertOne(studentInfo);
             }
        }

        long studentsCount = collection.countDocuments();
        System.out.println("Всего студентов в базе: " + studentsCount);

        BsonDocument queryAfterForty = BsonDocument.parse("{age: {$gt: 40}}");
        long studentsAfterForty = collection.countDocuments(queryAfterForty);
        System.out.println("Студентов старше 40 лет: " + studentsAfterForty);

        BsonDocument queryYangestStudent = BsonDocument.parse("{age : 1 }");
        MongoCursor<Document> cursor = collection.find().sort(queryYangestStudent).limit(1).cursor();
        String yangestStudentName = cursor.next().getString("name");
        System.out.println("Самый молодой студент: " + yangestStudentName);

        BsonDocument queryOlderStudent = BsonDocument.parse("{age : -1 }");
        Iterable<Document> olderStudentCourses = collection.find().sort(queryOlderStudent).limit(1);
        olderStudentCourses.forEach(student ->
                System.out.println("Курсы самого старого студента: " + student.get("courses")));
    }
}
