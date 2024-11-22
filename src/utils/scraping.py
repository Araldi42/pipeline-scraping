import re
from unidecode import unidecode

def gerar_url(pagina):
    if pagina == 1:
        return 'https://lista.mercadolivre.com.br/relogio#D[A:relogio]'
    else:
        offset = (pagina - 1) * 50 + 1
        return f'https://lista.mercadolivre.com.br/relogio_Desde_{offset}#D[A:relogio]'

def extrair_informacoes(texto):
    info = {}    
    # Extrair a marca (palavras em maiúsculas no início)
    # Extract 'marca' (brand)
    marca_match = re.search(r'Por\s+([^\s]+)', texto)
    if marca_match:
        info['marca'] = marca_match.group(1)
    else:
        # Fallback to uppercase words at the start
        marca_match = re.match(r'^([A-Z][A-Za-z]+)', texto)
        if marca_match:
            info['marca'] = marca_match.group(1)
        else:
            info['marca'] = ''
    # Extrair os valores
    valores = re.findall(r'R\$([\d\.]+,\d{2})', texto)
    if valores:
        # Valor total é o primeiro preço listado
        info['valor_total'] = valores[0].replace('.', '').replace(',', '.')
        # Se houver um segundo preço, é o valor com desconto
        if len(valores) > 1:
            info['valor_com_desconto'] = valores[1].replace('.', '').replace(',', '.')
        else:
            info['valor_com_desconto'] = info['valor_total']
    else:
        info['valor_total'] = '0.00'
        info['valor_com_desconto'] = '0.00'

    # Extrair o desconto em porcentagem
    desconto_match = re.search(r'\b(\d+)% OFF\b', texto)
    if desconto_match:
        info['desconto'] = desconto_match.group(1)
    else:
        info['desconto'] = '0'
    if info['valor_total'] != info['valor_com_desconto']:
        info['desconto'] = str(round((1 - float(info['valor_com_desconto']) / float(info['valor_total'])) * 100))
    
    # Extrair a descrição (remoção da marca e preços)
    descricao = texto
    # Remove 'Por [brand]' from the end
    descricao = re.sub(r'Por\s+[^\s]+$', '', descricao)
    # Remove ratings like '4.8(4923)'
    descricao = re.sub(r'\d\.\d\(\d+\)', '', descricao)
    # Remove prices
    descricao = re.sub(r'R\$\s*[\d\.]+,\d{2}', '', descricao)
    # Remove discounts
    descricao = re.sub(r'\b\d+% OFF\b', '', descricao)
    # Remove 'Por [brand]' from anywhere in the text
    descricao = re.sub(r'Por\s+[^\s]+', '', descricao)
    # Insert spaces where words are concatenated
    descricao = re.sub(r'([a-z])([A-Z])', r'\1 \2', descricao)
    # Replace multiple spaces with a single space
    descricao = re.sub(r'\s{2,}', ' ', descricao)

    info['descricao'] = descricao.strip()
    
    return info

def remove_zeros_from_left(data_row, column):
    '''Remove zeros à esquerda dos valores numéricos'''
    return data_row[column].lstrip('0')

def verify_zeros_from_left(data_row, column):
    '''Verifica se há zeros à esquerda nos valores numéricos'''
    if data_row[column][0] == '0':
        return True
    return False

def treat_data(data):
    '''Trata os dados para remoção de zeros à esquerda'''
    for item in data:
        if verify_zeros_from_left(item, 'valor_total'):
            item['valor_total'] = remove_zeros_from_left(item, 'valor_total')
        if verify_zeros_from_left(item, 'valor_com_desconto'):
            item['valor_com_desconto'] = remove_zeros_from_left(item, 'valor_com_desconto')
        if verify_zeros_from_left(item, 'desconto'):
            item['desconto'] = remove_zeros_from_left(item, 'desconto')
    return data

def validate_schema(data_row):
    '''Valida se a linha de dados possui o esquema correto'''
    if 'marca' not in data_row:
        return False
    if 'valor_total' not in data_row:
        return False
    if 'valor_com_desconto' not in data_row:
        return False
    if 'desconto' not in data_row:
        return False
    return True

def normalize_data(data):
    '''Normaliza os valores de texto para numéricos'''
    for item in data:
        item['marca'] = unidecode(item['marca']).replace(' ', '_').lower()
        item['valor_total'] = float(item['valor_total'])
        item['valor_com_desconto'] = float(item['valor_com_desconto'])
        item['desconto'] = item['desconto'].replace('%', '')
    return data
