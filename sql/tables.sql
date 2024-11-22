-- Tabela Client
CREATE TABLE Client (
    Client_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT,
    gender VARCHAR(50),
    address VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(20)
);

-- Tabela Product
CREATE TABLE Product (
    Product_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price FLOAT NOT NULL,
    category VARCHAR(100),
    quantity VARCHAR(50)
);

-- Tabela Transaction
CREATE TABLE Transaction (
    Transaction_id INT PRIMARY KEY,
    client_id INT,
    product_id INT,
    payment_method VARCHAR(100),
    quantity INT NOT NULL,
    date DATETIME NOT NULL,
    FOREIGN KEY (client_id) REFERENCES Client(Client_id),
    FOREIGN KEY (product_id) REFERENCES Product(Product_id)
);

CREATE TABLE MlData (
    id INT SERIAL PRIMARY KEY,
    marca VARCHAR(100),
    valor_total VARCHAR(100),
    valor_com_desconto VARCHAR(100),
    desconto VARCHAR(100),
);
