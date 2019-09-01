from flask import Flask, request, abort, make_response
from collections import deque
from lxml import etree
from dateutil.parser import parse
import fastjsonschema
import argparse

app = Flask(__name__)
q = deque()
DOCTYPE = '<?xml version="1.0" encoding="UTF-8"?>'


@app.after_request
def default_headers(response):
    response.headers["Content-Type"] = "application/xml"
    return response


def send_error(error_text):
    return f'{DOCTYPE}\n<Error>{error_text}</Error>'


@app.errorhandler(400)
def bad_request(error):
    return make_response(send_error('bad request'), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(send_error(f'{request.path} does not exist'), 404)


@app.errorhandler(405)
def not_allowed(error):
    return make_response(send_error(f'{request.method} is not allowed'), 405)


@app.errorhandler(500)
def internal_error(error):
    return make_response(send_error('Internal server error'), 500)


def isQueue(func):
    """Decorator for checking if Queue is empty"""
    def wrapped_func():
        if not len(q):
            return make_response(send_error('Queue is empty'), 404)
        return func()
    return wrapped_func


def validate_xml(received_xml):
    schema = etree.fromstring("""\
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
       <xs:element name="Message">
          <xs:complexType>
             <xs:sequence>
                <xs:element name="Header">
                   <xs:complexType>
                      <xs:sequence>
                         <xs:element name="To" type="xs:string" />
                         <xs:element name="From" type="xs:string" />
                         <xs:element name="Timestamp" type="xs:dateTime" />
                         <xs:element name="Title" type="xs:string" />
                         <xs:element name="Body" type="xs:string" />
                      </xs:sequence>
                   </xs:complexType>
                </xs:element>
             </xs:sequence>
          </xs:complexType>
       </xs:element>
    </xs:schema>
    """)
    validator_xml = etree.XMLSchema(schema)
    return validator_xml.validate(received_xml)


def validate_json(received_json):
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Filter",
        "description": "Find messages with filter",
        "type": "object",
        "properties": {
            "filter": {
                "type": "object",
                "minProperties": 1,
                "maxProperties": 4,
                "properties": {
                    "to": {
                        "type": "string"
                    },
                    "from": {
                        "type": "string"
                    },
                    "date": {
                        "type": "string"
                    },
                    "title": {
                        "type": "string"
                    }
                },
                "additionalProperties": False
            }
        },
        "required": ["filter"]
    }
    try:
        json_validate = fastjsonschema.compile(json_schema)
        json_validate(received_json)
        return 'valid'
    except fastjsonschema.exceptions.JsonSchemaException as e:
        return e.message


@app.route("/sendMessage", methods=['POST'])
def sendMessage():
    """Add message in Queue, if it matches the schema in validate_xml()
    and don't already exist"""
    try:
        item_xml = etree.fromstring(request.data)
    except etree.XMLSyntaxError:
        return abort(400)

    if validate_xml(item_xml):
        if len(q):
            for message in q:
                if etree.tostring(item_xml) == etree.tostring(message):
                    return make_response(send_error('message already exist'), 400)

        q.append(item_xml)
        return make_response('', 201)
    else:
        return make_response(send_error('xml is incorrect'), 422)


@app.route("/getMessage")
@isQueue
def getMessage():
    """Delete message from Queue"""
    item = q.popleft()
    return make_response('', 204)


def apply_filter(json_data):
    """Find message in Queue with json-filter"""
    s = json_data['filter']

    paths = {'to': 'To',
             'from': 'From',
             'title': 'Title',
             'date': 'Timestamp'}
    common_keys = paths.keys() & s.keys()

    pack_message = etree.Element("Messages")
    for message in q:
        expr = "//*[local-name() = $name]"
        check_mssg = lambda key: message.xpath(expr, name=paths[key])[0].text
        check_date = lambda date: parse(check_mssg(date)).strftime('%d.%m.%Y')

        match = all([check_date(key) == s[key] if key == 'date' else
                     s[key] == check_mssg(key) for key in common_keys])

        if match:
            pack_message.append(message)

    return pack_message if len(pack_message) else None


@app.route("/findMessages", endpoint='findMessages')
@isQueue
def findMessages():
    """Find message in Queue"""
    filter_json = request.get_json()
    res = validate_json(filter_json)
    if res == 'valid':
        pack_message = apply_filter(filter_json)
        if pack_message is not None:
            return make_response(etree.tostring(pack_message, doctype=DOCTYPE), 200)
        else:
            return make_response(send_error('message not found'), 404)
    else:
        return make_response(send_error(res), 400)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', default="1234")
    args = parser.parse_args()
    port = int(args.port)
    app.run(host='127.0.0.1', port=port)
