#!/bin/bash

# Inicializa variável do modo reverso
REVERSE_MODE=0
FILE_PATH=""
DATA_REFERENCIA=""

# Processa os argumentos
for arg in "$@"; do
    case "$arg" in
        -r)
            REVERSE_MODE=1
            ;;
        *)
            if [ -z "$FILE_PATH" ]; then
                FILE_PATH="$arg"
            elif [ -z "$DATA_REFERENCIA" ]; then
                DATA_REFERENCIA="$arg"
            fi
            ;;
    esac
done

# Verifica se os argumentos obrigatórios foram informados
if [ -z "$FILE_PATH" ] || [ -z "$DATA_REFERENCIA" ]; then
    echo "Erro: Caminho do arquivo e data de referência são obrigatórios."
    echo "Uso: $0 [-r] <caminho_do_arquivo> <data_referencia>"
    echo "Exemplo (espera o arquivo ser criado): $0 /caminho/arquivo.txt \"2024-02-06\""
    echo "Exemplo (espera o arquivo ser deletado): $0 -r /caminho/arquivo.txt \"2024-02-06\""
    exit 1
fi

# Define a condição inicial do arquivo (existe ou não)
FILE_EXISTS=0
if [ -e "$FILE_PATH" ]; then
    FILE_EXISTS=1
fi

echo "Monitorando: $FILE_PATH"
echo "Data de referência: $DATA_REFERENCIA"
echo "Modo reverse: $REVERSE_MODE"

# Calcula o tempo limite (Data de referência + 1 dia às 07:00)
TIME_LIMIT=$(date -d "$DATA_REFERENCIA +1 day 07:00" +%s)
CURRENT_TIME=$(date +%s)

echo "Tempo limite para monitoramento: $(date -d @$TIME_LIMIT '+%Y-%m-%d %H:%M:%S')"
echo ""

# Lógica XOR para inverter a condição de espera
while [ $(( FILE_EXISTS ^ REVERSE_MODE )) -eq 1 ]; do
    sleep 1  # Aguarda 1 segundo antes de verificar novamente

    # Atualiza o estado do arquivo
    if [ -e "$FILE_PATH" ]; then
        FILE_EXISTS=1
    else
        FILE_EXISTS=0
    fi

    # Atualiza o tempo atual
    CURRENT_TIME=$(date +%s)

    # Se ultrapassar o tempo limite, encerra o script
    if [ "$CURRENT_TIME" -ge "$TIME_LIMIT" ]; then
        echo "Tempo limite atingido! Nenhuma alteração detectada até $(date -d @$TIME_LIMIT '+%Y-%m-%d %H:%M:%S')."
        exit 1
    fi
done

# Mensagem final baseada no modo
if [ "$REVERSE_MODE" -eq 1 ]; then
    echo "Arquivo $FILE_PATH foi REMOVIDO."
else
    echo "Arquivo $FILE_PATH foi CRIADO."
fi
