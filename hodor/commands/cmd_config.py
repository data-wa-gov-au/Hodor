import click
import urllib
from retries import retries
from hodor.cli import pass_context
from pprintpp import pprint as pp

@click.command('config', short_help='Configuration utility for proxy servers, and other user-specific settings.')
@pass_context
def cli(ctx):
  # Detect whether we need the user to configure a proxy.
  proxy_info = urllib.getproxies()
  if len(proxy_info) == 0:
    print "It looks like you're behind a proxy server..."
    pp(proxy_info)
    proxy_host = "ph"
    proxy_port = "pp"

    proxy_username = click.prompt('Please enter your username')
    proxy_password = click.prompt('Please enter your password', hide_input=True, confirmation_prompt=True)

    print proxy_host
    print proxy_port
    print proxy_username
    print proxy_password
    return


  # from pbkdf2 import crypt
  # import json
  # somejson = json.dumps({"pass": 1})
  # pwhash = crypt(somejson)
  # print pwhash
  # if pwhash == crypt(somejson, pwhash):
  #   print "OK"
  # else:
  #   print "BAD"



  # import urllib
  # pp(urllib.getproxies())
  # # {'http': 'http://proxy.institution.edu:8080', 'https': 'http://proxy.institution.edu:8080'}
  # exit()
  #
  # # httplib2.Http()
  # # http://httplib2.googlecode.com/hg/doc/html/libhttplib2.html#httplib2.ProxyInfo
  # http = httplib2.Http(proxy_info=httplib2.ProxyInfo(
  #   httplib2.socks.PROXY_TYPE_HTTP_NO_TUNNEL,
  #   'proxy.dli.wa.gov.au',
  #   8080,
  #   proxy_user = '',
  #   proxy_pass = ''
  # ))
