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
#include <unistd.h>
#include <cstring>

using json = nlohmann::json;
using namespace std;

// Classe CDatabase
class CDatabase
{
public:
    CDatabase(const string& dbHost, const string& dbUser, const string& dbPassword, const string& dbName)
        : dbHost_(dbHost), dbUser_(dbUser), dbPassword_(dbPassword), dbName_(dbName),
          mysql_(nullptr, mysql_close)
    {
        connectToDatabase();
    }

    ~CDatabase()
    {
        disconnectFromDatabase();
    }

    vector<pair<string, int>> getGatewayNamesAndProtocols();
    int getDeviceId(const string& gatewayName);
    void insertMessageData(const string& gatewayName, time_t utcTimestamp, double differenceKWh, int deviceId);

private:
    string dbHost_;
    string dbUser_;
    string dbPassword_;
    string dbName_;
    unique_ptr<MYSQL, decltype(&mysql_close)> mysql_;

    void connectToDatabase();
    void disconnectFromDatabase();
};

vector<pair<string, int>> CDatabase::getGatewayNamesAndProtocols()
{
    vector<pair<string, int>> gatewayProtocols;
    string query = "SELECT Nom_dispositif, ID_Protocole_FK FROM Dispositif_Passerelle";
    mysql_query(mysql_.get(), query.c_str());

    MYSQL_RES* result = mysql_store_result(mysql_.get());
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        gatewayProtocols.emplace_back(row[0], stoi(row[1]));
    }
    mysql_free_result(result);
    return gatewayProtocols;
}

int CDatabase::getDeviceId(const string& gatewayName)
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

void CDatabase::insertMessageData(const string& gatewayName, time_t utcTimestamp, double differenceKWh, int deviceId)
{
    stringstream queryStream;
    queryStream << "INSERT INTO Donnee_Mesurer (Timestamp, Valeur_Mesure, ID_Dispositif_FK) VALUES (FROM_UNIXTIME("
                << utcTimestamp << "), " << differenceKWh << ", " << deviceId << ")";

    const string& query = queryStream.str();
    mysql_query(mysql_.get(), query.c_str());
}

void CDatabase::connectToDatabase()
{
    mysql_.reset(mysql_init(nullptr));

    if (mysql_real_connect(mysql_.get(), dbHost_.c_str(), dbUser_.c_str(), dbPassword_.c_str(), dbName_.c_str(), 0, nullptr, 0)) {
        cout << "Connecte a la base de donnees." << endl;
    } else {
        cout << "Erreur de connexion a la base de donnees." << endl;
    }
}

void CDatabase::disconnectFromDatabase()
{
    mysql_.reset();
}

// Classe MqttClient
class MqttClient : public mosqpp::mosquittopp
{
public:
    MqttClient(const string& clientId, const string& mqttHost, int mqttPort, CDatabase& dbManager)
        : mosquittopp(clientId.c_str()), mqttHost_(mqttHost), mqttPort_(mqttPort), dbManager_(dbManager), messageCount_(0)
    {
        connectToMqttBroker();
        startMqttLoop();
        lastMessageTime_ = chrono::system_clock::now();
        cout << "Client MQTT connecte et pret." << endl;
    }

    ~MqttClient()
    {
        stopMqttLoop();
        disconnectFromMqttBroker();
    }

    void on_connect(int rc) override;
    void on_message(const struct mosquitto_message* message) override;

    chrono::system_clock::time_point getLastMessageTime() const
    {
        return lastMessageTime_;
    }

private:
    string mqttHost_;
    int mqttPort_;
    CDatabase& dbManager_;
    unordered_map<string, vector<double>> lastLoadValuesKwh_;
    chrono::system_clock::time_point lastMessageTime_;
    int messageCount_; // Counter for the number of messages received

    void connectToMqttBroker();
    void startMqttLoop();
    void stopMqttLoop();
    void disconnectFromMqttBroker();

    bool isRelevantTopic(const string& topic);
    void processIncomingMessage(const string& topic, const string& payload);
    void displayMessageInTerminal(const string& payload);
    void insertMessageIntoBDD(const string& topic, const string& payload);
    string extractGatewayNameFromTopic(const string& topic);
};

void MqttClient::on_connect(int rc)
{
    if (rc == 0)
    {
        cout << "Connecte au broker MQTT." << endl;
        vector<pair<string, int>> gatewayProtocols = dbManager_.getGatewayNamesAndProtocols();
        for (const auto& gatewayProtocol : gatewayProtocols)
        {
            const string& gatewayName = gatewayProtocol.first;
            int protocolId = gatewayProtocol.second;

            if (protocolId == 1)
            {
                string topic = "energy/consumption/" + gatewayName + "/message/data/71435500-6791-11ce-97c6-313131303230";
                subscribe(nullptr, topic.c_str());
                cout << "Abonne au sujet : " << topic << endl;
            }
            else
            {
                cout << "Passerelle " << gatewayName << " ignoree en raison de ID_Protocole_FK = " << protocolId << endl;
            }
        }
    }
    else
    {
        cout << "Erreur de connexion au broker MQTT : " << rc << endl;
    }
}

void MqttClient::on_message(const struct mosquitto_message* message)
{
    string topic = message->topic;
    string payload(static_cast<char*>(message->payload), message->payloadlen);

    if (isRelevantTopic(topic))
    {
        cout << "Message recu sur le sujet : " << topic << endl;
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

bool MqttClient::isRelevantTopic(const string& topic)
{
    return topic.find("energy/consumption/") != string::npos;
}

void MqttClient::processIncomingMessage(const string& topic, const string& payload)
{
    displayMessageInTerminal(payload);
    insertMessageIntoBDD(topic, payload);
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

void MqttClient::insertMessageIntoBDD(const string& topic, const string& payload)
{
    auto jsonData = json::parse(payload);

    time_t utcTimestamp = jsonData["utctimestamp"];
    double currentLoadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;

    string gatewayName = extractGatewayNameFromTopic(topic);
    int deviceId = dbManager_.getDeviceId(gatewayName);

    if (deviceId == -1)
    {
        return;
    }

    // Stocker les trois dernières valeurs
    if (lastLoadValuesKwh_.find(gatewayName) == lastLoadValuesKwh_.end())
    {
        lastLoadValuesKwh_[gatewayName] = {};
    }

    auto& values = lastLoadValuesKwh_[gatewayName];
    if (values.size() < 3)
    {
        values.push_back(currentLoadValueKWh);
    }
    else
    {
        values.erase(values.begin());
        values.push_back(currentLoadValueKWh);
    }

    if (values.size() == 6)
    {
        double diff1 = values[1] - values[0];
        double diff2 = values[2] - values[1];
        double diff3 = values[3] - values[2];
        double diff4 = values[4] - values[3];
        double diff5 = values[5] - values[4];

        double sumDiff = diff1 + diff2 + diff3 + diff4 + diff5;

        messageCount_++;

        if (messageCount_ >= 1)
        {
            dbManager_.insertMessageData(gatewayName, utcTimestamp, sumDiff, deviceId);
            cout << "Donnees inserees dans la bdd." << endl;
            cout << "==============================" << endl;

            // Conserver la dernière valeur et effacer les autres pour recommencer le processus
            double lastValue = values.back();
            values.clear();
            values.push_back(lastValue);
            messageCount_ = 0; // Réinitialiser le compteur après insertion
        }
    }
    else
    {
        cout << "Pas assez de valeurs pour " << gatewayName << ", insertion ignoree." << endl;
        cout << "===================================================" << endl;
    }
}


string MqttClient::extractGatewayNameFromTopic(const string& topic)
{
    size_t start = topic.find("/", 18) + 1;
    size_t end = topic.find("/", start);
    return topic.substr(start, end - start);
}

// Fonction principale
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

    CDatabase dbManager(dbHost, dbUser, dbPassword, dbName);
    MqttClient mqttClient(mqttClientId, mqttHost, mqttPort, dbManager);

    while (running)
    {
        this_thread::sleep_for(chrono::seconds(60)); // Vérification toutes les minutes

        auto now = chrono::system_clock::now();
        auto lastMessageTime = mqttClient.getLastMessageTime();
        auto duration = chrono::duration_cast<chrono::seconds>(now - lastMessageTime);

        if (duration.count() > 660) // 11 minutes
        {
            cout << "Aucun message recu depuis 11 minutes, redemarrage complet du programme..." << endl;
            // Remplacement de l'image du processus par une nouvelle instance du programme
            execvp(argv[0], argv);

            // Si execvp échoue, on affiche l'erreur et on sort de la boucle
            cerr << "Erreur lors du redemarrage : " << strerror(errno) << endl;
            break;
        }
    }
    return 0;
}
