# Kafka ECS Idempotent Processor

Processador de eventos de metadados de tabelas (rodando em ECS) que consome de um tópico Kafka, garante idempotência baseada no estado do payload usando DynamoDB, e arquiva os eventos no S3, notificando via SQS. Construído com Python, Docker e Arquitetura Limpa.

---

### Arquitetura

O serviço segue um fluxo de processamento de eventos desacoplado e idempotente.

```mermaid
graph TD
    subgraph "Ambiente Docker Local"
        Producer("Produtor Kafka") -- "Evento JSON (UPSERT/DROP)" --> Kafka
        PythonApp["Serviço Python (Consumidor)"] -- "Consome evento" --> Kafka
        PythonApp -- "1. Calcula hash do payload" --> PythonApp
        PythonApp -- "2. Lê lastStateHash" --> DynamoDB
        subgraph "Decisão de Idempotência"
            direction LR
            A("incoming_hash == last_hash?")
        end
        PythonApp -- "3. Compara hashes" --> Decisão
        Decisão -- "Sim (Duplicado) --> Ignora"
        Decisão -- "Não (Novo Estado) --> Processa"
        subgraph "Processamento (Novo Estado)"
            direction TB
            PythonApp -- "4. Salva JSON no S3" --> S3
            PythonApp -- "5. Envia path do S3 via SQS" --> SQS
            PythonApp -- "6. Atualiza lastStateHash" --> DynamoDB
        end
    end
```

### Principais Características

* **Idempotência Baseada em Estado:** A lógica não apenas previne eventos duplicados, mas também processa apenas mudanças reais no estado da estrutura de uma tabela (comparando o hash do payload).
* **Arquitetura Limpa (Clean Architecture):** A lógica de negócio é isolada de detalhes de infraestrutura (frameworks, bancos de dados), promovendo alta testabilidade e manutenibilidade.
* **100% Dockerizado:** Todos os serviços (Python App, Kafka, Zookeeper, LocalStack) são executados em contêineres Docker, garantindo um ambiente de desenvolvimento consistente e fácil de configurar.
* **Simulação de AWS com LocalStack:** S3, SQS e DynamoDB são simulados localmente, permitindo o desenvolvimento completo sem custos e sem a necessidade de uma conta AWS.

### Stack de Tecnologia

* **Linguagem:** Python 3.10+
* **Infraestrutura:** Docker & Docker Compose
* **Mensageria:** Kafka
* **Serviços AWS (simulados):** S3, SQS, DynamoDB
* **Bibliotecas Principais:** `kafka-python`, `boto3`, `python-dotenv`

---

### Estrutura do Projeto

O projeto segue os princípios da Arquitetura Limpa, separando o código em camadas:

* `src/app/core`: Contém as `entities` (regras de negócio) e `use_cases` (orquestração da lógica).
* `src/app/presentation`: Define as `ports` (interfaces) que o core utiliza.
* `src/app/infrastructure`: Implementa as `adapters` (ex: repositório DynamoDB), `entrypoints` (ex: consumidor Kafka) e `settings`.

---

### Como Executar Localmente

**Pré-requisitos:**
* Docker
* Docker Compose

**1. Clone o Repositório**
```bash
git clone [https://github.com/SEU_USUARIO/kafka-ecs-idempotent-processor.git](https://github.com/SEU_USUARIO/kafka-ecs-idempotent-processor.git)
cd kafka-ecs-idempotent-processor
```

**2. Configure as Variáveis de Ambiente**
Copie o arquivo de exemplo e mantenha os valores padrão para o ambiente local.
```bash
cp .env.example .env
```

**3. Suba os Contêineres**
Este comando irá construir a imagem do seu aplicativo Python e iniciar todos os serviços (Kafka, LocalStack, etc).
```bash
docker-compose up --build -d
```
Aguarde um minuto para que todos os serviços estejam prontos.

**4. Crie os Recursos no LocalStack**
Você precisa criar a infraestrutura AWS (simulada) na primeira vez que executar. Use a [AWS CLI](https://aws.amazon.com/cli/) com o wrapper `awslocal` ou execute os comandos dentro do contêiner do LocalStack.

```bash
# Instale awslocal se não tiver: pip install awscli-local

# Criar a tabela DynamoDB
aws dynamodb create-table \
    --table-name idempotency-keys \
    --attribute-definitions AttributeName=tableIdentifier,AttributeType=S \
    --key-schema AttributeName=tableIdentifier,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

# Criar o bucket S3
aws s3api create-bucket --bucket processed-events

# Criar a fila SQS
aws sqs create-queue --queue-name event_notifications
```

**5. Crie o Tópico Kafka**
```bash
docker-compose exec kafka kafka-topics --create \
    --topic events \
    --bootstrap-server kafka:9092 \
    --partitions 1 \
    --replication-factor 1
```

---

### Como Testar

Para testar o fluxo, envie uma mensagem para o tópico Kafka. Você pode usar `kafkacat` ou o console producer do próprio Kafka.

**Exemplo usando `docker-compose exec`:**

1.  **Envie o primeiro evento (Criação):**
    ```bash
    docker-compose exec kafka bash -c "echo '{\"event_id\":\"uuid-1\",\"event_type\":\"UPSERT\",\"instance_name\":\"prod-rds-1\",\"db_name\":\"sales\",\"table_name\":\"customers\",\"columns\":[{\"name\":\"id\",\"data_type\":\"int\",\"is_nullable\":false},{\"name\":\"email\",\"data_type\":\"varchar(100)\",\"is_nullable\":false}]}' | kafka-console-producer --topic events --bootstrap-server kafka:9092"
    ```
    * **Resultado esperado:** A mensagem será processada, um arquivo JSON será criado no S3, uma mensagem SQS será enviada e o hash será gravado no DynamoDB. Verifique os logs com `docker-compose logs -f app`.

    **DynamoDB:** Verifique se o estado foi salvo.
    ```bash
    aws dynamodb scan --table-name idempotency-keys
    # Esperado: um item com o 'tableIdentifier' e um 'lastStateHash' calculado.
    ```

    **S3:** Verifique se o arquivo JSON foi criado
    ```bash
    aws s3 ls s3://processed-events/prod-rds-1/sales/customers/
    # Esperado: um arquivo chamado uuid-1.json
    ```

    **SQS:** Verifique se a notificação foi enviada.
    ```bash
    awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/event_notifications
    # Esperado: uma mensagem contendo o path para o arquivo uuid-1.json no S3.
    ```

2.  **Envie o mesmo evento (Duplicado):**
    * Execute o mesmo comando acima.
    * **Resultado esperado:** O log do serviço mostrará "Duplicate state detected... Skipping.". Nada acontecerá no S3 ou SQS.

3.  **Envie um evento de atualização (coluna nova):**
    ```bash
    docker-compose exec kafka bash -c "echo '{
        \"event_id\": \"uuid-2\",
        \"event_type\": \"UPSERT\",
        \"instance_name\": \"prod-rds-1\",
        \"db_name\": \"sales\",
        \"table_name\": \"customers\",
        \"columns\": [
            {\"name\": \"id\", \"data_type\": \"int\", \"is_nullable\": false},
            {\"name\": \"email\", \"data_type\": \"varchar(100)\", \"is_nullable\": false},
            {\"name\": \"created_at\", \"data_type\": \"timestamp\", \"is_nullable\": true}
        ]
    }' | kafka-console-producer --topic events --bootstrap-server kafka:9092"
    ```
    * **Resultado esperado:** Como o hash do payload é diferente, o evento será processado.

4.  **Envie um evento de DROP:**
    ```bash
    docker-compose exec kafka bash -c "echo '{
        \"event_id\": \"uuid-3\",
        \"event_type\": \"DROP\",
        \"instance_name\": \"prod-rds-1\",
        \"db_name\": \"sales\",
        \"table_name\": \"customers\"
    }' | kafka-console-producer --topic events --bootstrap-server kafka:9092"
    ```
    * **Resultado esperado:** O evento de DROP será processado. O estado no DynamoDB será atualizado para `"DROPPED"`.