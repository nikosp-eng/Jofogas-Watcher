from main import get_data, get_sql_connection, get_from_mysql, upload_to_mysql, products_to_html
from flask import Flask, render_template, request


app = Flask(__name__)

HEADERS = ({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})
# Add a proxy (optional)
PROXY = ''
TIMEOUT_IN_SECONDS = 3


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/products', methods=['POST'])
def products():
    keyword = request.form.get('keyword')
    filter_name = request.form.get('filter_name')
    if keyword or filter_name:
        try:
            if keyword:
                products, note = get_data(
                    keyword, PROXY, HEADERS, TIMEOUT_IN_SECONDS)
                upload_to_mysql(get_sql_connection(), products)
                results = products_to_html(products)
            if filter_name:
                note = ""
                results = get_from_mysql(get_sql_connection(), filter_name)
        except ValueError:
            return "Please enter a valid item"
    else:
        return render_template('index.html')
    return render_template("products.html", data=results, note=note, style="width:100%")


if __name__ == '__main__':
    app.run(debug=True)
