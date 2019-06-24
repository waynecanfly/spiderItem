month_periods = dict.fromkeys(range(1, 13), 'Q')
month_periods.update({
    3: 'Q1',
    6: 'Q2',
    9: 'Q3',
    12: 'Q4'
})

title_mappings = {
    'Balanço Patrimonial Ativo': 'BS1',
    'Balanço Patrimonial Passivo': 'BS2',
    'Demonstração do Resultado': 'IS',
    'Demonstração do Resultado Abrangente': 'OCI',
    'Demonstração do Fluxo de Caixa': 'CF'
}

report_type_titles = {v: k for k, v in title_mappings.items()}
