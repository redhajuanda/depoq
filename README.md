# depoq

depoq is a simple SQL file-based rep


The main goal of this library is to bring back readability and clarity by separating Go code from SQL queries. Instead of intertwining complex SQL logic within Go structs or methods, this library allows you to define and manage your SQL queries independently, keeping your codebase cleaner and more maintainable. By isolating the database interaction layer, it’s easier to focus on business logic in Go without having to constantly dig through ORM-generated queries or deal with the lack of transparency. This approach not only improves the readability of your code but also gives you full control over your SQL queries, so you can optimize them without worrying about hidden abstractions.