import sqlparse
from pygments import highlight
from pygments.lexers import SqlLexer, BashLexer
from pygments.formatters import TerminalFormatter


def sql_print(sql):
    formatted_sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    highlighted_sql = highlight(formatted_sql, SqlLexer(), TerminalFormatter())
    print(highlighted_sql)
    return None


def bash_print(shell):
    highlighted_shell = highlight(shell, BashLexer(), TerminalFormatter())
    print(highlighted_shell)
    return None
