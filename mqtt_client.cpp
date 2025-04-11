#include <iostream>
#include <sstream>
#include <iomanip>
#include <mosquittopp.h>
#include <csignal>
#include <thread>
#include <string>
#include <optional>
#include "nlohmann/json.hpp"
#include <ctime>
#include <mysql.h>
#include <memory>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <unistd.h>    // Pour execvp
#include <cstring>     // Pour strerror

using json = nlohmann::json;
using namespace std;

class MqttClient : public mosqpp::mosquittopp
{
public:
    MqttClient(const string& clientId, const string& mqttHost, int mqttPort,
               const string& dbHost, const string& dbUser, const string& dbPassword, const string& dbName)
        : mosquittopp(clientId.c_str()), mqttHost_(mqttHost), mqttPort_(mqttPort),
          dbHost_(dbHost), dbUser_(dbUser), dbPassword_(dbPassword), dbName_(dbName),
          mysql_(nullptr, mysql_close)
    {
        connectToMqttBroker();
        startMqttLoop();
        connectToDatabase();
        lastMessageTime_ = chrono::system_clock::now();
    }

    ~MqttClient()
    {
        stopMqttLoop();
        disconnectFromMqttBroker();
        disconnectFromDatabase();
    }

    vector<pair<string, int>> getGatewayNamesAndProtocolsFromDatabase();

    void on_connect(int rc) override;
    void on_message(const struct mosquitto_message* message) override;

    chrono::system_clock::time_point getLastMessageTime() const {
        return lastMessageTime_;
    }

private:
    string mqttHost_;
    int mqttPort_;
    string dbHost_;
    string dbUser_;
    string dbPassword_;
    string dbName_;
    unique_ptr<MYSQL, decltype(&mysql_close)> mysql_;
    unordered_map<string, optional<double>> lastLoadValuesKwh_;
    chrono::system_clock::time_point lastMessageTime_;

    void connectToMqttBroker();
    void startMqttLoop();
    void stopMqttLoop();
    void disconnectFromMqttBroker();

    void connectToDatabase();
    void disconnectFromDatabase();

    bool isRelevantTopic(const string& topic);
    void processIncomingMessage(const string& topic, const string& payload);
    void displayMessageInTerminal(const string& payload);
    void insertMessageDataIntoDatabase(const string& topic, const string& payload);
    string extractGatewayNameFromTopic(const string& topic);
    int getDeviceIdFromDatabase(const string& gatewayName);
};

vector<pair<string, int>> MqttClient::getGatewayNamesAndProtocolsFromDatabase()
{
    vector<pair<string, int>> gatewayProtocols;
    string query = "SELECT Nom_dispositif, ID_Protocole_FK FROM Dispositif_Passerelle";
    mysql_query(mysql_.get(), query.c_str());

    MYSQL_RES* result = mysql_store_result(mysql_.get());
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)))
    {
        gatewayProtocols.emplace_back(row[0], stoi(row[1]));
    }
    mysql_free_result(result);
    return gatewayProtocols;
}

void MqttClient::on_connect(int rc)
{
    if (rc == 0)
    {
        vector<pair<string, int>> gatewayProtocols = getGatewayNamesAndProtocolsFromDatabase();
        for (const auto& gatewayProtocol : gatewayProtocols)
        {
            const string& gatewayName = gatewayProtocol.first;
            int protocolId = gatewayProtocol.second;

            if (protocolId == 1)
            {
                string topic = "energy/consumption/" + gatewayName + "/message/data/71435500-6791-11ce-97c6-313131303230";
                subscribe(nullptr, topic.c_str());
            }
            else
            {
                cout << "Passerelle " << gatewayName << " ignoree en raison de ID_Protocole_FK = " << protocolId << endl;
            }
        }
    }
}

void MqttClient::on_message(const struct mosquitto_message* message)
{
    string topic = message->topic;
    string payload(static_cast<char*>(message->payload), message->payloadlen);

    if (isRelevantTopic(topic))
    {
        processIncomingMessage(topic, payload);
        lastMessageTime_ = chrono::system_clock::now();
    }
}

void MqttClient::connectToMqttBroker()
{
    connect_async(mqttHost_.c_str(), mqttPort_, 60);
}

void MqttClient::startMqttLoop()
{
    loop_start();
}

void MqttClient::stopMqttLoop()
{
    loop_stop();
}

void MqttClient::disconnectFromMqttBroker()
{
    disconnect();
}

void MqttClient::connectToDatabase()
{
    mysql_.reset(mysql_init(nullptr));
    mysql_real_connect(mysql_.get(), dbHost_.c_str(), dbUser_.c_str(), dbPassword_.c_str(), dbName_.c_str(), 0, nullptr, 0);
}

void MqttClient::disconnectFromDatabase()
{
    mysql_.reset();
}

bool MqttClient::isRelevantTopic(const string& topic)
{
    return topic.find("energy/consumption/") != string::npos;
}

void MqttClient::processIncomingMessage(const string& topic, const string& payload)
{
    displayMessageInTerminal(payload);
    insertMessageDataIntoDatabase(topic, payload);
}

void MqttClient::displayMessageInTerminal(const string& payload)
{
    if (json::accept(payload))
    {
        auto jsonData = json::parse(payload);

        if (jsonData.contains("utctimestamp"))
        {
            time_t timeVal = jsonData["utctimestamp"];
            cout << "UTC Timestamp: " << put_time(gmtime(&timeVal), "%Y-%m-%d %H:%M:%S") << endl;
        }

        if (jsonData["measures"].contains("Load_0_30001"))
        {
            double loadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;
            cout << "Load_0_30001: " << loadValueKWh << " kWh" << endl;
        }
    }
}

void MqttClient::insertMessageDataIntoDatabase(const string& topic, const string& payload)
{
    auto jsonData = json::parse(payload);

    time_t utcTimestamp = jsonData["utctimestamp"];
    double currentLoadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;

    string gatewayName = extractGatewayNameFromTopic(topic);
    int deviceId = getDeviceIdFromDatabase(gatewayName);

    if (deviceId == -1)
    {
        return;
    }

    if (lastLoadValuesKwh_.count(gatewayName))
    {
        double differenceKWh = currentLoadValueKWh - lastLoadValuesKwh_[gatewayName].value();

        stringstream queryStream;
        queryStream << "INSERT INTO Donnee_Mesurer (Timestamp, Valeur_Mesure, ID_Dispositif_FK) VALUES (FROM_UNIXTIME("
                    << utcTimestamp << "), " << differenceKWh << ", " << deviceId << ")";

        const string& query = queryStream.str();
        mysql_query(mysql_.get(), query.c_str());

        cout << "Donnees inserees dans la bdd." << endl;
        cout << "==============================" << endl;
    }
    else
    {
        cout << "Premiere valeur recue pour " << gatewayName << ", insertion ignoree." << endl;
        cout << "===================================================" << endl;
    }

    lastLoadValuesKwh_[gatewayName] = currentLoadValueKWh;
}

string MqttClient::extractGatewayNameFromTopic(const string& topic)
{
    size_t start = topic.find("/", 18) + 1;
    size_t end = topic.find("/", start);
    return topic.substr(start, end - start);
}

int MqttClient::getDeviceIdFromDatabase(const string& gatewayName)
{
    string query = "SELECT ID_Dispositif_PK FROM Dispositif_Passerelle WHERE Nom_dispositif = '" + gatewayName + "' LIMIT 1";
    mysql_query(mysql_.get(), query.c_str());
    MYSQL_RES* result = mysql_store_result(mysql_.get());
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row)
    {
        mysql_free_result(result);
        return -1;
    }
    int deviceId = stoi(row[0]);
    mysql_free_result(result);
    return deviceId;
}

// Variable globale de contrôle du programme
volatile sig_atomic_t running = 1;

void signalHandler(int sig)
{
    running = 0;
}

int main(int argc, char* argv[])
{
    signal(SIGINT, signalHandler);

    const string mqttHost = "217.182.60.210";
    int mqttPort = 1883;
    const string mqttClientId = "cpp_mqtt_client";

    const string dbHost = "217.182.60.210";
    const string dbUser = "admin";
    const string dbPassword = "admin";
    const string dbName = "Mesure_De";

    cout << "Demarrage du client MQTT..." << endl;
    MqttClient mqttClient(mqttClientId, mqttHost, mqttPort, dbHost, dbUser, dbPassword, dbName);

    while (running)
    {
        this_thread::sleep_for(chrono::seconds(1));

        auto now = chrono::system_clock::now();
        auto lastMessageTime = mqttClient.getLastMessageTime();
        auto duration = chrono::duration_cast<chrono::seconds>(now - lastMessageTime);

        cout << "Secondes sans message : " << duration.count() << endl;

        if (duration.count() > 60)
        {
            cout << "Aucun message recu depuis 1 minute, redemarrage complet du programme..." << endl;
            // Remplacement de l'image du processus par une nouvelle instance du programme
            execvp(argv[0], argv);
            
            // Si execvp échoue, on affiche l'erreur et on sort de la boucle
            cerr << "Erreur lors du redemarrage : " << strerror(errno) << endl;
            break;
        }
    }

    return 0;
}
