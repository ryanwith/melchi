def normalize_sql(sql):
    return ' '.join(sql.replace('\n', ' ').split()).strip()