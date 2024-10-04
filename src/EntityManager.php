<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\ORM\Swoole;

use Closure;
use DateTimeInterface; // phpcs:ignore
use Doctrine\Common\EventManager;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\LockMode; // phpcs:ignore
use Doctrine\ORM;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\EntityRepository;
use Doctrine\ORM\Exception\ORMException;
use Doctrine\ORM\Internal\Hydration\AbstractHydrator;
use Doctrine\ORM\Mapping\ClassMetadata;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\PessimisticLockException;
use Doctrine\ORM\Query\ResultSetMapping;
use RuntimeException;
use Swoole\Coroutine as Co;

final class EntityManager implements EntityManagerInterface
{
    public function __construct(private Closure $emCreatorFn)
    {
    }

    public function getWrappedEm() : EntityManagerInterface
    {
        $context = $this->getContext();
        if (! isset($context[self::class]) || ! $context[self::class] instanceof EntityManagerInterface) {
            /** @psalm-var EntityManagerInterface */
            $context[self::class] = ($this->emCreatorFn)();
            /** @psalm-suppress MixedMethodCall */
            Co::defer(static function () use ($context) {
                // After reopen context missing
                if (isset($context[self::class]) && $context[self::class] instanceof EntityManagerInterface) {
                    $context[self::class]->close();
                    unset($context[self::class]);
                }
            });
        }
        /** @psalm-var EntityManagerInterface */
        return $context[self::class];
    }

    /**
     * @psalm-suppress MethodSignatureMismatch
     */
    public function getClassMetadata(string $className) : ClassMetadata
    {
        return $this->getWrappedEm()->getClassMetadata($className);
    }

    public function getUnitOfWork() : ORM\UnitOfWork
    {
        return $this->getWrappedEm()->getUnitOfWork();
    }

    /**
     * @psalm-suppress all
     */
    public function getRepository(string $className) : EntityRepository
    {
        $metadata            = $this->getClassMetadata($className);
        $repositoryClassName = $metadata->customRepositoryClassName
            ?: $this->getConfiguration()->getDefaultRepositoryClassName();

        return new $repositoryClassName($this, $metadata);
    }

    public function getConnection() : Connection
    {
        return $this->getWrappedEm()->getConnection();
    }

    public function getCache() : ?ORM\Cache
    {
        return $this->getWrappedEm()->getCache();
    }

    public function getExpressionBuilder() : ORM\Query\Expr
    {
        return $this->getWrappedEm()->getExpressionBuilder();
    }

    public function beginTransaction() : void
    {
        $this->getWrappedEm()->beginTransaction();
    }

    public function wrapInTransaction(callable $func) : mixed
    {
        return $this->getWrappedEm()->wrapInTransaction($func);
    }

    public function commit() : void
    {
        $this->getWrappedEm()->commit();
    }

    public function rollback() : void
    {
        $this->getWrappedEm()->rollback();
    }

    public function createQuery(string $dql = '') : ORM\Query
    {
        return $this->getWrappedEm()->createQuery($dql);
    }

    public function createNativeQuery(string $sql, ResultSetMapping $rsm) : ORM\NativeQuery
    {
        return $this->getWrappedEm()->createNativeQuery($sql, $rsm);
    }

    public function createQueryBuilder() : ORM\QueryBuilder
    {
        return $this->getWrappedEm()->createQueryBuilder();
    }

    /**
     * @throws ORMException
     */
    public function getReference(string $entityName, mixed $id) : ?object
    {
        return $this->getWrappedEm()->getReference($entityName, $id);
    }

    public function close() : void
    {
        $this->getWrappedEm()->close();
    }

    /**
     * @throws OptimisticLockException
     * @throws PessimisticLockException
     */
    public function lock(object $entity, LockMode|int $lockMode, DateTimeInterface|int|null $lockVersion = null) : void
    {
        $this->getWrappedEm()->lock($entity, $lockMode, $lockVersion);
    }

    public function getEventManager() : EventManager
    {
        return $this->getWrappedEm()->getEventManager();
    }

    public function getConfiguration() : ORM\Configuration
    {
        return $this->getWrappedEm()->getConfiguration();
    }

    public function isOpen() : bool
    {
        return $this->getWrappedEm()->isOpen();
    }

    /**
     * @throws ORMException
     */
    public function newHydrator(int|string $hydrationMode) : AbstractHydrator
    {
        return $this->getWrappedEm()->newHydrator($hydrationMode);
    }

    public function getProxyFactory() : ORM\Proxy\ProxyFactory
    {
        return $this->getWrappedEm()->getProxyFactory();
    }

    public function getFilters() : ORM\Query\FilterCollection
    {
        return $this->getWrappedEm()->getFilters();
    }

    public function isFiltersStateClean() : bool
    {
        return $this->getWrappedEm()->isFiltersStateClean();
    }

    public function hasFilters() : bool
    {
        return $this->getWrappedEm()->hasFilters();
    }

    public function find(
        string $className,
        mixed $id,
        LockMode|int|null $lockMode = null,
        ?int $lockVersion = null
    ) : ?object {
        return $this->getWrappedEm()->find($className, $id, $lockMode, $lockVersion);
    }

    public function persist(object $object) : void
    {
        $this->getWrappedEm()->persist($object);
    }

    public function remove(object $object) : void
    {
        $this->getWrappedEm()->remove($object);
    }

    public function clear() : void
    {
        $this->getWrappedEm()->clear();
    }

    public function detach(object $object) : void
    {
        $this->getWrappedEm()->detach($object);
    }

    public function refresh(object $object, LockMode|int|null $lockMode = null) : void
    {
        $this->getWrappedEm()->refresh($object, $lockMode);
    }

    public function flush() : void
    {
        $this->getWrappedEm()->flush();
    }

    public function getMetadataFactory() : ORM\Mapping\ClassMetadataFactory
    {
        return $this->getWrappedEm()->getMetadataFactory();
    }

    public function initializeObject(object $obj) : void
    {
        $this->getWrappedEm()->initializeObject($obj);
    }

    public function contains(object $object) : bool
    {
        return $this->getWrappedEm()->contains($object);
    }

    public function reopen() : void
    {
        $context = $this->getContext();
        $em      = $this->getWrappedEm();
        if ($em->isOpen()) {
            $em->clear();
        } else {
            unset($context[self::class]);
        }
    }

    private function getContext() : Co\Context
    {
        /** @psalm-var Co\Context|null $context */
        $context = Co::getContext((int) Co::getCid());
        if (! $context instanceof Co\Context) {
            throw new RuntimeException('Co::Context unavailable');
        }
        return $context;
    }
}
