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

    private static final String FILE_PATH = "src/main/data/mongo.csv";
    private static final String DATABASE = "local";
    private static final String KEY = "Students";

    public static void main(String[] args) {

        MongoCollection<Document> collection = createCollection(DATABASE, KEY);

        try {
            fillDatabase(FILE_PATH, collection);
        }
        catch (Exception ex) {
            System.out.println("Файл для создания базы не существует или не читается");
            return;
        }

            getStudentsCount(collection);
            getStudentsAfterForty(collection);
            getYangestStudentName(collection);
            getOlderStudentCourses(collection);
    }

    public static MongoCollection<Document> createCollection(String databaseName, String collectionKey)
    {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionKey);
        collection.drop();

        return collection;
    }

    public static void fillDatabase(String path, MongoCollection<Document> collection) throws IOException {

            List<String> lines = Files.readAllLines(Paths.get(path));

            for (int i = 0; i < lines.size(); i++)
            {
                Pattern pattern = Pattern.compile("(?<name>.+),(?<age>[0-9]+),(?<courses>.+)");
                Matcher matcher = pattern.matcher(lines.get(i));
                if (matcher.find()) {
                    Document studentInfo = new Document()
                            .append("name", matcher.group("name"))
                            .append("age", Integer.parseInt(matcher.group("age")))
                            .append("courses", matcher.group("courses"));
                    collection.insertOne(studentInfo);
                } else {
                    System.out.println("Строка №" + i + " не соответствует необходимому формату и не может быть обработана");
                }
            }
    }

    public static void getStudentsCount(MongoCollection<Document> collection)
    {
        System.out.println("Всего студентов в базе: " + collection.countDocuments());
    }

    public static void getStudentsAfterForty(MongoCollection<Document> collection)
    {
        BsonDocument queryAfterForty = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("Студентов старше 40 лет: " + collection.countDocuments(queryAfterForty));
    }

    public static void getYangestStudentName(MongoCollection<Document> collection)
    {
        BsonDocument queryYangestStudent = BsonDocument.parse("{age : 1 }");
        MongoCursor<Document> cursor = collection.find().sort(queryYangestStudent).limit(1).cursor();
        System.out.println("Самый молодой студент: " + cursor.next().getString("name"));
    }

    public static void getOlderStudentCourses(MongoCollection<Document> collection)
    {
        BsonDocument queryOlderStudent = BsonDocument.parse("{age : -1 }");
        Iterable<Document> olderStudentCourses = collection.find().sort(queryOlderStudent).limit(1);
        olderStudentCourses.forEach(student ->
                System.out.println("Курсы самого старого студента: " + student.get("courses")));
    }
}