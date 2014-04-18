package simpledb;

/**
 * Simple class to represent Tables in SimpleDB.
 */
public class Table {
    private DbFile file;        // DbFile which stores the table's contents
    private String name;        // Name of the table
    private String pkeyField;   // Primary key of the table

    // Constructor
    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    // Accessors
    public DbFile get_file() { return this.file; }
    public String get_name() { return this.name; }
    public String get_pkeyField() { return this.pkeyField; }
}
