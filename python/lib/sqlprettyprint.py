import sqlparse
from pygments import highlight
from pygments.lexers import SqlLexer, BashLexer
from pygments.formatters import TerminalFormatter


def sql_print(sql):
    # Format the SQL query using sqlparse
    formatted_sql = sqlparse.format(sql, reindent=True, keyword_case="upper")

    # Apply syntax highlighting using pygments
    highlighted_sql = highlight(formatted_sql, SqlLexer(), TerminalFormatter())

    # Print the formatted and highlighted SQL code
    print(highlighted_sql)

    return None


def bash_print(shell):
    # Apply syntax highlighting using pygments
    highlighted_shell = highlight(shell, BashLexer(), TerminalFormatter())

    # Print the formatted and highlighted SQL code
    print(highlighted_shell)

    return None
