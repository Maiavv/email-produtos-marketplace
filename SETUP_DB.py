import sqlite3

def criar_tabelas(nome_arquivo_db):
    conexao = sqlite3.connect(nome_arquivo_db)
    cursor = conexao.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Produtos (
        id_produto TEXT PRIMARY KEY,
        nome TEXT NOT NULL,
        marca TEXT,
        ean TEXT UNIQUE
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Lojas (
        id_loja INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_loja TEXT NOT NULL UNIQUE
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS PrecosDiarios (
        id_preco INTEGER PRIMARY KEY AUTOINCREMENT,
        id_produto TEXT,
        id_loja INTEGER,
        data DATE NOT NULL,
        preco DECIMAL NOT NULL,
        list_price DECIMAL,
        is_available BOOLEAN,
        available_quantity INTEGER,
        preco_sem_desconto DECIMAL,
        FOREIGN KEY (id_produto) REFERENCES Produtos (id_produto),
        FOREIGN KEY (id_loja) REFERENCES Lojas (id_loja)
    );
    ''')

    conexao.commit()
    conexao.close()

criar_tabelas("dados_concorrentes.db")
