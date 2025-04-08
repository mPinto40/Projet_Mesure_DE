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

using json = nlohmann::json;
using namespace std;

class MqttClient : public mosqpp::mosquittopp
{
public:
    MqttClient(const string& clientId, const string& mqttHost, int mqttPort,
               const string& dbHost, const string& dbUser, const string& dbPassword, const string& dbName)
        : mosquittopp(clientId.c_str()), mqttHost_(mqttHost), mqttPort_(mqttPort),
          dbHost_(dbHost), dbUser_(dbUser), dbPassword_(dbPassword), dbName_(dbName) {
        connectToMqttBroker();
        startMqttLoop();
        connectToDatabase();
    }

    ~MqttClient()
    {
        stopMqttLoop();
        disconnectFromMqttBroker();
        disconnectFromDatabase();
    }

    vector<string> getGatewayNamesFromDatabase()
    {
        vector<string> gatewayNames;

        if (!isDatabaseConnected())
        {
            connectToDatabase();
            if (!isDatabaseConnected())
            {
                cerr << "Échec de la connexion à MySQL, impossible de récupérer les noms des passerelles." << endl;
                return gatewayNames;
            }
        }

        string query = "SELECT Nom_dispositif FROM Dispositif_Passerelle";
        if (mysql_query(mysql_.get(), query.c_str()))
        {
            cerr << "Échec de la requête MySQL : " << mysql_error(mysql_.get()) << endl;
            disconnectFromDatabase();
            return gatewayNames;
        }

        MYSQL_RES* result = mysql_store_result(mysql_.get());
        if (result)
        {
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(result)))
            {
                gatewayNames.push_back(row[0]);
            }
            mysql_free_result(result);
        }

        return gatewayNames;
    }

    void on_connect(int rc) override
    {
        if (rc == 0)
        {
            vector<string> gatewayNames = getGatewayNamesFromDatabase();
            if (!gatewayNames.empty())
            {
                for (const auto& gatewayName : gatewayNames)
                {
                    string topic = "energy/consumption/" + gatewayName + "/message/data/71435500-6791-11ce-97c6-313131303230";
                    subscribe(nullptr, topic.c_str());
                    cout << "Abonné au sujet : " << topic << endl;
                }
            }
            else
            {
                cerr << "Aucun nom de passerelle trouvé." << endl;
            }
        }
    }

    void on_message(const struct mosquitto_message* message) override
    {
        string topic = message->topic;
        string payload(static_cast<char*>(message->payload), message->payloadlen);

        if (isRelevantTopic(topic))
        {
            processIncomingMessage(topic, payload);
        }
    }

private:
    string mqttHost_;
    int mqttPort_;
    string dbHost_;
    string dbUser_;
    string dbPassword_;
    string dbName_;
    unique_ptr<MYSQL, decltype(&mysql_close)> mysql_{nullptr, mysql_close};
    unordered_map<string, optional<double>> lastLoadValuesKwh_;

    void connectToMqttBroker()
    {
        connect_async(mqttHost_.c_str(), mqttPort_, 60);
    }

    void startMqttLoop()
    {
        loop_start();
    }

    void stopMqttLoop()
    {
        loop_stop();
    }

    void disconnectFromMqttBroker()
    {
        disconnect();
    }

    void connectToDatabase()
    {
        mysql_.reset(mysql_init(nullptr));
        if (mysql_real_connect(mysql_.get(), dbHost_.c_str(), dbUser_.c_str(), dbPassword_.c_str(), dbName_.c_str(), 0, nullptr, 0) == nullptr) {
            cerr << "Échec de la connexion MySQL : " << mysql_error(mysql_.get()) << endl;
            mysql_.reset();
        }
    }

    void disconnectFromDatabase()
    {
        mysql_.reset();
    }

    bool isDatabaseConnected()
    {
        return mysql_ && mysql_ping(mysql_.get()) == 0;
    }

    bool isRelevantTopic(const string& topic)
    {
        return topic.find("energy/consumption/") != string::npos;
    }

    void processIncomingMessage(const string& topic, const string& payload)
    {
        displayMessageInTerminal(payload);
        insertMessageDataIntoDatabase(topic, payload);
    }

    void displayMessageInTerminal(const string& payload)
    {
        try {
            auto jsonData = json::parse(payload);

            if (jsonData.contains("utctimestamp"))
            {
                time_t time = jsonData["utctimestamp"];
                cout << "UTC Timestamp: " << put_time(gmtime(&time), "%Y-%m-%d %H:%M:%S") << endl;
            }
            else
            {
                cout << "UTC Timestamp non disponible." << endl;
            }

            if (jsonData["measures"].contains("Load_0_30001"))
            {
                double loadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;
                cout << "Load_0_30001: " << loadValueKWh << " kWh" << endl;
            }
            else
            {
                cout << "Load_0_30001 non disponible." << endl;
            }
        }
        catch (const json::parse_error& e)
        {
            cerr << "Erreur d'analyse JSON : " << e.what() << endl;
        }
    }

    void insertMessageDataIntoDatabase(const string& topic, const string& payload)
    {
        if (!isDatabaseConnected())
        {
            connectToDatabase();
            if (!isDatabaseConnected())
            {
                cerr << "Échec de la connexion à MySQL, impossible d'insérer les données." << endl;
                return;
            }
        }

        try
        {
            auto jsonData = json::parse(payload);
            if (jsonData.contains("utctimestamp") && jsonData["measures"].contains("Load_0_30001"))
            {
                time_t utcTimestamp = jsonData["utctimestamp"];
                double currentLoadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;

                string gatewayName = extractGatewayNameFromTopic(topic);
                int deviceId = getDeviceIdFromDatabase(gatewayName);

                if (lastLoadValuesKwh_[gatewayName] && deviceId != -1)
                {
                    double differenceKWh = currentLoadValueKWh - *lastLoadValuesKwh_[gatewayName];
                    stringstream queryStream;
                    queryStream << "INSERT INTO Donnee_Mesurer (Timestamp, Valeur_Mesure, ID_Dispositif_FK) VALUES (FROM_UNIXTIME("
                                << utcTimestamp << "), " << differenceKWh << ", " << deviceId << ")";
                    string query = queryStream.str();

                    if (mysql_query(mysql_.get(), query.c_str()))
                    {
                        cerr << "Échec de la requête MySQL : " << mysql_error(mysql_.get()) << endl;
                        disconnectFromDatabase();
                    }
                    else
                    {
                        cout << "Données insérées dans la base de données." << endl;
                        cout << "==============================" << endl;
                    }
                }
                else
                {
                    cout << "Première valeur reçue ou ID du dispositif non trouvé, pas d'insertion dans la base de données." << endl;
                    cout << "===================================================" << endl;
                }

                lastLoadValuesKwh_[gatewayName] = currentLoadValueKWh;
            }
        } catch (const json::parse_error& e)
        {
            cerr << "Erreur d'analyse JSON pour l'insertion dans la base de données : " << e.what() << endl;
        }
    }

    string extractGatewayNameFromTopic(const string& topic) 
    {
        size_t start = topic.find("/", 18) + 1;
        size_t end = topic.find("/", start);
        return topic.substr(start, end - start);
    }

    int getDeviceIdFromDatabase(const string& gatewayName)
    {
        int deviceId = -1;

        if (!isDatabaseConnected())
        {
            connectToDatabase();
            if (!isDatabaseConnected())
            {
                cerr << "Échec de la connexion à MySQL, impossible de récupérer l'ID du dispositif." << endl;
                return -1;
            }
        }

        string query = "SELECT ID_Dispositif_PK FROM Dispositif_Passerelle WHERE Nom_dispositif = '" + gatewayName + "' LIMIT 1";
        if (mysql_query(mysql_.get(), query.c_str()))
        {
            cerr << "Échec de la requête MySQL : " << mysql_error(mysql_.get()) << endl;
            disconnectFromDatabase();
            return -1;
        }

        MYSQL_RES* result = mysql_store_result(mysql_.get());
        if (result && mysql_num_rows(result) > 0)
        {
            MYSQL_ROW row = mysql_fetch_row(result);
            deviceId = stoi(row[0]);
        }

        mysql_free_result(result);
        return deviceId;
    }
};

volatile sig_atomic_t running = 1;

void signalHandler(int sig)
{
    running = 0;
}

int main()
{
    signal(SIGINT, signalHandler);

    const string mqttHost = "217.182.60.210";
    int mqttPort = 1883;
    const string mqttClientId = "cpp_mqtt_client";

    const string dbHost = "217.182.60.210";
    const string dbUser = "admin";
    const string dbPassword = "admin";
    const string dbName = "Mesure_De";

    MqttClient mqttClient(mqttClientId, mqttHost, mqttPort, dbHost, dbUser, dbPassword, dbName);

    while (running)
    {
        this_thread::sleep_for(chrono::seconds(1));
    }

    return 0;
}
