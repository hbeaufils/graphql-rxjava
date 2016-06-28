package graphql;

import graphql.execution.batched.BatchedDataFetcher;
import graphql.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;

/**
 * @author Hadrien Beaufils
 * @date 2016.06.17
 */
public class SchemaFixture {

    private static final Logger log = LoggerFactory.getLogger( SchemaFixture.class );

    public static class TypeName {
        public static final String COMPANY = "Company";
        public static final String PRODUCT = "Product";
        public static final String TRANSACTION = "Transaction";
        public static final String HISTORY_LINE = "HistoryLine";
    }

    public static class Company {

        String id;
        String name;
        Product product;

        public Company(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public Company(String id, String name, Product product) {
            this.id = id;
            this.name = name;
            this.product = product;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Product getProduct() {
            return product;
        }
    }

    public static class Product {

        String name;
        String serialNumber;

        public Product(String name, String serialNumber) {
            this.name = name;
            this.serialNumber = serialNumber;
        }

        public String getName() {
            return name;
        }

        public String getSerialNumber() {
            return serialNumber;
        }
    }

    public static class Transaction {

        String token;
        String email;
        String fullName;

        public Transaction(String token, String email, String fullName) {
            this.token = token;
            this.email = email;
            this.fullName = fullName;
        }

        public String getToken() {
            return token;
        }

        public String getEmail() {
            return email;
        }

        public String getFullName() {
            return fullName;
        }
    }

    public static class HistoryLine {

        String date;
        String type;
        String description;

        public enum Type {
            CREATE, UPDATE
        }

        public HistoryLine(String date, String type, String description) {
            this.date = date;
            this.type = type;
            this.description = description;
        }

        public String getDate() {
            return date;
        }

        public String getType() {
            return type;
        }

        public String getDescription() {
            return description;
        }
    }
    
    // ====== Data ========================================================== //

    public static final HistoryLine creationEvent = new HistoryLine(
            "2016-06-16T15:04:07.166Z", HistoryLine.Type.CREATE.toString(),
            "Day of the creation"
    );
    public static final HistoryLine updateForTransactionsAAAA = new HistoryLine(
            "2016-06-16T15:04:07.166Z", HistoryLine.Type.UPDATE.toString(),
            "Some small update"
    );

    public static final Transaction t1a = new Transaction("1AAAA","john.snow@test.com","John Snow");
    public static final Transaction t1b = new Transaction("1BBBB","jane.smith@test.com","Jane Smith");
    public static final Transaction t2a = new Transaction("2AAAA","sarah.connor@skynet.com","Sarah Connor");
    public static final Transaction t2b = new Transaction("2BBBB","sherlock.holmes@test.com","Sherlock Holmes");

    public static final Product p1 = new Product("My Product A","123");
    public static final Product p2 = new Product("My Product B","456");

    public static final Company c1 = new Company("0","company 1");
    public static final Company c2 = new Company("1","company 2");
}
