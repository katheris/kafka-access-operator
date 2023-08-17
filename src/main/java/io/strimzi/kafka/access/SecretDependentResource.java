package io.strimzi.kafka.access;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.kafka.access.internal.CustomResourceParseException;
import io.strimzi.kafka.access.internal.KafkaAccessParser;
import io.strimzi.kafka.access.internal.KafkaListener;
import io.strimzi.kafka.access.internal.KafkaParser;
import io.strimzi.kafka.access.model.KafkaAccess;
import io.strimzi.kafka.access.model.KafkaAccessSpec;
import io.strimzi.kafka.access.model.KafkaReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@KubernetesDependent(labelSelector = KafkaAccessParser.MANAGED_BY_LABEL_KEY + "=" + KafkaAccessParser.KAFKA_ACCESS_LABEL_VALUE,
        resourceDiscriminator = SecretResourceDescriminator.class)
public class SecretDependentResource extends CRUDKubernetesDependentResource<Secret, KafkaAccess> {

    private static final String TYPE_SECRET_KEY = "type";
    private static final String TYPE_SECRET_VALUE = "kafka";
    private static final String PROVIDER_SECRET_KEY = "provider";
    private static final String PROVIDER_SECRET_VALUE = "strimzi";
    private static final String SECRET_TYPE = "servicebinding.io/kafka";
    private static final String ACCESS_SECRET_EVENT_SOURCE = "access-secret-event-source";
    private final Map<String, String> commonSecretData = new HashMap<>();
    private final Map<String, String> commonSecretLabels = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(SecretDependentResource.class);

    public SecretDependentResource() {
        super(Secret.class);
        final Base64.Encoder encoder = Base64.getEncoder();
        commonSecretData.put(TYPE_SECRET_KEY, encoder.encodeToString(TYPE_SECRET_VALUE.getBytes(StandardCharsets.UTF_8)));
        commonSecretData.put(PROVIDER_SECRET_KEY, encoder.encodeToString(PROVIDER_SECRET_VALUE.getBytes(StandardCharsets.UTF_8)));
        commonSecretLabels.put(KafkaAccessParser.MANAGED_BY_LABEL_KEY, KafkaAccessParser.KAFKA_ACCESS_LABEL_VALUE);
    }

    @Override
    protected Secret desired(final KafkaAccess kafkaAccess, final Context<KafkaAccess> context) {
        final KafkaReference kafkaReference = kafkaAccess.getSpec().getKafka();
        final String kafkaClusterName = kafkaReference.getName();
        final String kafkaClusterNamespace = Optional.ofNullable(kafkaReference.getNamespace()).orElse(kafkaAccess.getMetadata().getNamespace());
        final InformerEventSource<Kafka, KafkaAccess> kafkaEventSource = (InformerEventSource<Kafka, KafkaAccess>) context.eventSourceRetriever().getResourceEventSourceFor(Kafka.class);
        final Kafka kafka = kafkaEventSource.get(new ResourceID(kafkaClusterName, kafkaClusterNamespace))
                .orElseThrow(() -> new IllegalStateException(String.format("Kafka %s/%s missing", kafkaClusterNamespace, kafkaClusterName)));
        final Map<String, String> data  = secretData(kafkaAccess.getSpec(), kafka);
        return new SecretBuilder()
                .withType(SECRET_TYPE)
                .withNewMetadata()
                    .withName(kafkaAccess.getMetadata().getName())
                    .withNamespace(kafkaAccess.getMetadata().getNamespace())
                    .withLabels(commonSecretLabels)
                .endMetadata()
                .withData(data)
                .build();
    }

    protected Map<String, String> secretData(final KafkaAccessSpec spec, final Kafka kafka) {
        final Map<String, String> data  = new HashMap<>(commonSecretData);
        final KafkaListener listener;
        try {
//            if (kafkaUserReference.isPresent()) {
//                if (!KafkaUser.RESOURCE_KIND.equals(kafkaUserReference.get().getKind()) || !KafkaUser.RESOURCE_GROUP.equals(kafkaUserReference.get().getApiGroup())) {
//                    throw new CustomResourceParseException(String.format("User kind must be %s and apiGroup must be %s", KafkaUser.RESOURCE_KIND, KafkaUser.RESOURCE_GROUP));
//                }
//                final String kafkaUserName = kafkaUserReference.get().getName();
//                final String kafkaUserNamespace = Optional.ofNullable(kafkaUserReference.get().getNamespace()).orElse(kafkaAccessNamespace);
//                final KafkaUser kafkaUser = getKafkaUser(kafkaUserName, kafkaUserNamespace);
//                final String kafkaUserType = Optional.ofNullable(kafkaUser)
//                        .map(KafkaUser::getSpec)
//                        .map(KafkaUserSpec::getAuthentication)
//                        .map(KafkaUserAuthentication::getType)
//                        .orElse(KafkaParser.USER_AUTH_UNDEFINED);
//                listener = KafkaParser.getKafkaListener(kafka, spec, kafkaUserType);
//                if (kafkaUser != null) {
//                    final KafkaUserData userData = Optional.ofNullable(kafkaUser.getStatus())
//                            .map(KafkaUserStatus::getSecret)
//                            .map(secretName -> kubernetesClient.secrets().inNamespace(kafkaUserNamespace).withName(secretName).get())
//                            .map(secret -> new KafkaUserData(kafkaUser).withSecret(secret))
//                            .orElse(new KafkaUserData(kafkaUser));
//                    data.putAll(userData.getConnectionSecretData());
//                }
//
//            } else {
                listener = KafkaParser.getKafkaListener(kafka, spec);
//            }
        } catch (CustomResourceParseException e) {
            throw new IllegalStateException("Reconcile failed due to ParserException " + e.getMessage());
        }
        data.putAll(listener.getConnectionSecretData());
        return data;
    }
}
