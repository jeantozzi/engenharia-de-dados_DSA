# Custo de Implementação de um Data Lake

Para o Lab 1, o objetivo será compreender o custo de implementação de um Data Lake em dois diferentes cenários.

Apresentaremos as premissas para precificação, a relação de custo dos componentes/serviços e então um resumo geral.

## Premissas

1. Serão considerados os custos de 2 cenários: on-premise e em nuvem. Implementação híbrida ou alguma outra solução não estão sendo consideradas.
2. Todos os custos estarão em USD (dólares americanos).
3. Vamos considerar os custos principais de hardware, serviço e consultoria. Custos complementares podem ser necessários.
4. Vamos trazer uma estimativa de alto nível. O custo detalhado pode envolver outros fatores dependendo da empresa/cliente.
5. A visão do custo é de um consultor implementando um Data Lake para uma empresa cliente.

## Relação de custos
### Cenário 1 - Implementação on-premise
<table>
    <thead>
        <tr>
            <th>Componente</th>
            <th>Custo</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Cluster - 3 Masters (16 cores, 128 GB RAM, 2x 2 TB)</td>
            <td rowspan=2>~ US$ 80.000</td>
        </tr>
        <tr>
            <td>Cluster - 7 Nodes (16 cores, 256 GB RAM, 12x 2 TB)</td>
        </tr>
        <tr>
            <td>Rede 10Gbps, Softwares de Monitoramento, Custos de CPD, Storage, Switches, Suporte de Hardware</td>
            <td>~ US$ 25.000</td>
        </tr>
        <tr>
            <td>Instalação, Configuração, Testes, Integração, Automatização</td>
            <td>~ US$ 35.000</td>
        </tr>
        <tr>
            <td><strong>Custo Anual Total</strong></td>
            <td><strong>~ US$ 140.000</strong></td>
        </tr>
        <tr>
            <td><strong>Custo Mensal</strong></td>
            <td><strong>~ US$ 12.000</strong></td>
        </tr>
    </tbody>
</table>

### Cenário 2 - Implementação em nuvem (Microsoft Azure)
<table>
    <thead>
        <tr>
            <th>Componente</th>
            <th>Custo</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Azure Data Lake Storage (150 TB)</td>
            <td>~ US$ 5.500</td>
        </tr>
        <tr>
            <td>HDInsight Cluster (10 compute nodes)</td>
            <td>~ US$ 3.800</td>
        </tr>
        <tr>
            <td>Suporte Enterprise</td>
            <td>~ US$ 1.000</td>
        </tr>
        <tr>
            <td>Instalação, Configuração, Testes, Integração, Automatização</td>
            <td>~ US$ 30.000</td>
        </tr>
        <tr>
            <td><strong>Custo no Primeiro Mês</strong></td>
            <td><strong>~ US$ 40.300</strong></td>
        </tr>
        <tr>
            <td><strong>Custo Mensal Subsequente</strong></td>
            <td><strong>~ US$ 10.300</strong></td>
        </tr>
    </tbody>
</table>

## Resumo geral

A implementação em nuvem mostra, na maioria dos cenários, um custo menor comparado à implementação on-premise.

De qualquer forma, 2 aspectos são importantes quando se trata de implementação em nuvem:
1. A possibilidade de desligar/diminuir funcionalidades e serviços (seja por falta de uso ou necessidade) faz com que os gastos sejam mais controlados.
2. Na ocasião de não continuidade do projeto de Data Lake, o provisionamento é excluído e os custos acabam, ao contrário da implementação on-premise, que ainda precisaria se preocupar com todo o equipamento e as licenças adiquiridos.