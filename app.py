from flask import Flask
from flask import url_for
from flask import request
from flask import redirect
from flask import render_template
from py2neo import Graph

app = Flask(__name__)
uri = "neo4j+s://2d75c36b.databases.neo4j.io:7687"
user = "neo4j"
password = "H1CP8qvwySUmRgtDfsu7PXjRv4RlLWlntzLt0bTi0NE"

graph = Graph(uri, auth = (user, password))

@app.route('/')
def index():
    return render_template('index.html', customers=get_customers())

@app.route('/customer_menu/<id>')
def show_customer_menu(id):
    return render_template('customer_menu.html', id=id)

@app.route('/loans/<id>')
def show_loans(id):
    return render_template('loans.html', loans=get_loans(id))

@app.route('/credit_cards/<id>')
def show_credit_card(id):
    return render_template('credit_cards.html', credit_card=get_credit_card(id))

@app.route('/accounts/<id>')
def show_account(id):
    return render_template('accounts.html', account=get_account(id))

@app.route('/customer_all/<id>')
def show_customer_all(id):
    return render_template('customer_all.html', data=get_all_data(id))

@app.route('/transactions/withdraw_from/<id>')
def show_transactions_withdraw_from_account(id):
    return render_template('transactions.html', transactions=get_transactions_withdraw_from_account(id), withdraw=True)

@app.route('/transactions/deposit_to/<id>')
def show_transactions_deposit_to_account(id):
    return render_template('transactions.html', transactions=get_transactions_deposit_to_account(id), withdraw=False)

@app.route('/new_customer')
def new_customer():
    return render_template('new_customer.html')

@app.route('/delete_customer/<id>')
def delete_customer(id):
    query = "MATCH(c:Customer {customer_id: '" + id + "'}) DETACH DELETE c"
    graph.run(query)
    return redirect(url_for('index'))

@app.route('/add_customer', methods=["GET","POST"])
def add_customer():
    if request.method == "POST":
        id = request.json['id']
        name = request.json['name']
        acct_id = request.json['acct_id']
        acct_role = request.json['acct_role']
        cc_num = request.json['cc_num']
        loan_ids = request.json['loan_ids']
        deposit_transaction_ids = request.json['deposit_transaction_ids']
        withdraw_transaction_ids = request.json['withdraw_transaction_ids']

        query = "MATCH (c:Customer) WHERE c.customer_id = '" + id + "' RETURN c.customer_id"
        is_customer_exist = graph.run(query)

        if not is_customer_exist.forward():
            query = "CREATE (c:Customer {customer_id: '" + id + "', name: '" + name + "'})"
            graph.run(query)

            if acct_id:
                add_account(id, acct_id, acct_role, deposit_transaction_ids, withdraw_transaction_ids)

            if loan_ids[0]:
                add_loans(id, loan_ids)

            if cc_num:
                add_credit_card(id, cc_num)

        return redirect(url_for('index'))
    else:
        return redirect(url_for('index'))

def add_credit_card(id, cc_num):
    query = "MATCH (cc:CreditCard) WHERE cc.cc_num = '" + cc_num + "' RETURN cc.cc_num"
    is_credit_card_exist = graph.run(query)

    if not is_credit_card_exist.forward():
        query = "CREATE (cc:CreditCard {cc_num: '" + cc_num + "'})"
        graph.run(query)

        query = "MATCH (c:Customer), (cc:CreditCard) " \
                "WHERE c.customer_id = '" + id + "' AND cc.cc_num = '" + cc_num + \
                "' CREATE (c)-[:USES]->(cc)"
        graph.run(query)

def add_loans(id, loan_ids):
    for loan_id in loan_ids:
        query = "MATCH (l:Loan) WHERE l.loan_id = '" + loan_id + "' RETURN l.loan_id"
        is_loan_exist = graph.run(query)

        if not is_loan_exist.forward():
            query = "CREATE (l:Loan {loan_id: '" + loan_id + "'})"
            graph.run(query)

        query = "MATCH (c:Customer), (l:Loan) " \
                "WHERE c.customer_id = '" + id + "' AND l.loan_id = '" + loan_id + \
                "' CREATE (c)-[:OWES]->(l)"
        graph.run(query)

def add_account(id, acct_id, acct_role, deposit_transaction_ids, withdraw_transaction_ids):
    query = "MATCH (a:Account) WHERE a.acct_id = '" + acct_id + "' RETURN a.acct_id"
    is_account_exist = graph.run(query)

    if not is_account_exist.forward():
        query = "CREATE (a:Account {acct_id: '" + acct_id + "'})"
        graph.run(query)

    query = "MATCH (c:Customer), (a:Account) " \
            "WHERE c.customer_id = '" + id + "' AND a.acct_id = '" + acct_id + \
            "' CREATE (c)-[:OWNS {role: '" + acct_role + "'}]->(a)"
    graph.run(query)

    if deposit_transaction_ids:
        add_transactions("DEPOSIT_TO", acct_id, deposit_transaction_ids)

    if withdraw_transaction_ids:
        add_transactions("WITHDRAW_FROM", acct_id, withdraw_transaction_ids)

def add_transactions(type, acct_id, transaction_ids):
    if transaction_ids[0]:
        for transaction_id in transaction_ids:
            query = "MATCH (t:Transaction) WHERE t.transaction_id = '" + transaction_id + "' RETURN t.transaction_id"
            is_transaction_exist = graph.run(query)

            if not is_transaction_exist.forward():
                query = "CREATE (t:Transaction {transaction_id: '" + transaction_id + "'})"
                graph.run(query)

        query = "MATCH "
        for transaction_id in transaction_ids:
            query += "(t" + transaction_id + ":Transaction), "

        query += "(a:Account) WHERE "
        amount = 0
        for transaction_id in transaction_ids:
            query += "t" + transaction_id + ".transaction_id = '" + transaction_id + "' AND "
            amount += 1

        query += " a.acct_id = '" + acct_id + "' CREATE "
        counter = 0
        for transaction_id in transaction_ids:
            counter += 1
            query += "(t" + transaction_id + ")-[:" + type + "]->(a)"
            if counter < amount:
                query += ", "

        graph.run(query)

def get_customers():
    query = "Match(c:Customer) RETURN c.customer_id as id, c{.*} ORDER BY c.customer_id"
    customers = graph.run(query).data()
    if customers:
        return customers
    return None

def get_credit_card(id):
    query = "MATCH (c:Customer {customer_id: '" + id + "'})-[:USES]-(cc) RETURN cc{.*}"
    credit_card = graph.run(query).data()
    if credit_card:
        return credit_card[0]
    return None

def get_account(id):
    query = "MATCH (c:Customer {customer_id: '" + id + "'})-[:OWNS]-(a) RETURN a{.*}"
    account = graph.run(query).data()
    if account:
        return account[0]
    return None

def get_loans(id):
    query = "MATCH (c:Customer {customer_id: '" + id + "'})-[:OWES]-(l) RETURN l{.*}"
    loans = graph.run(query).data()
    if loans:
        return loans
    return None

def get_all_data(id):
    query = "MATCH (c:Customer {customer_id: '" + id + "'})-[r]-(n) RETURN c{.*}, r{.*}, n{.*}"
    data_from_query = graph.run(query).data()

    if data_from_query:
        result = {}
        loans = []
        result['name'] = data_from_query[0]['c']['name']
        result['customer_id'] = data_from_query[0]['c']['customer_id']

        for data in data_from_query:
            if 'cc_num' in data['n']:
                result['cc_num'] = data['n']['cc_num']

            if 'role' in data['r'] and 'acct_id' in data['n']:
                result['acct_id'] = data['n']['acct_id']
                result['role'] = data['r']['role']

            if 'loan_id' in data['n']:
                loans.append(data['n']['loan_id'])

        result['loans'] = loans
        return result
    return None

def get_transactions_withdraw_from_account(id):
    query = "MATCH (c:Customer {customer_id: '" + id + "'})-[:OWNS]-(a) MATCH (t:Transaction) " \
            "WHERE EXISTS( (t)-[:WITHDRAW_FROM]-(a) ) RETURN t{.*}"
    data = graph.run(query).data()
    if data:
        return data
    return None

def get_transactions_deposit_to_account(id):
    query = "MATCH (c:Customer {customer_id: '" + id + "'})-[:OWNS]-(a) MATCH (t:Transaction) " \
            "WHERE EXISTS( (t)-[:DEPOSIT_TO]-(a) ) RETURN t{.*}"
    data = graph.run(query).data()
    if data:
        return data
    return None
