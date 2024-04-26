<?php

declare(strict_types=1);

namespace OpsWay\Doctrine\ORM\Swoole;

use Closure;
use DateTimeInterface;
use Doctrine\Common\EventManager;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\LockMode; // phpcs:ignore
use Doctrine\ORM;
use Doctrine\ORM\AbstractQuery; // phpcs:ignore
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\EntityRepository;
use Doctrine\ORM\Internal\Hydration\AbstractHydrator;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\Exception\ORMException;
use Doctrine\ORM\PessimisticLockException;
use Doctrine\ORM\Query\ResultSetMapping;
use Swoole\Coroutine as Co;

final class EntityManager implements EntityManagerInterface
{
    public function __construct(private Closure $emCreatorFn)
    {
    }

    public function getWrappedEm() : EntityManagerInterface
    {
        $cid = Co::getCid();
        $context = Co::getContext($cid);

        if (! isset($context->entityManager) || ! $context->entityManager instanceof EntityManagerInterface) {
            $context->entityManager = ($this->emCreatorFn)();
            Co::defer(function () use ($context) {
                $context->entityManager->close();
                unset($context->entityManager);
            });
        }

        return $context->entityManager;
    }

    /**
     * @param string $className
     */
    public function getClassMetadata(string $className) : ORM\Mapping\ClassMetadata
    {
        return $this->getWrappedEm()->getClassMetadata($className);
    }

    /**
     * @return ORM\UnitOfWork
     */
    public function getUnitOfWork() : ORM\UnitOfWork
    {
        return $this->getWrappedEm()->getUnitOfWork();
    }

    /**
     * @param string $className
     * @psalm-suppress all
     */
    public function getRepository(string $className) : EntityRepository
    {
        $metadata            = $this->getClassMetadata($className);
        $repositoryClassName = $metadata->customRepositoryClassName
            ?: $this->getConfiguration()->getDefaultRepositoryClassName();

        return new $repositoryClassName($this, $metadata);
    }

    /**
     * @return Connection
     */
    public function getConnection() : Connection
    {
        return $this->getWrappedEm()->getConnection();
    }

    /**
     * @return ORM\Cache|null
     */
    public function getCache() : ?ORM\Cache
    {
        return $this->getWrappedEm()->getCache();
    }

    /**
     * @return ORM\Query\Expr
     */
    public function getExpressionBuilder() : ORM\Query\Expr
    {
        return $this->getWrappedEm()->getExpressionBuilder();
    }

    public function beginTransaction() : void
    {
        $this->getWrappedEm()->beginTransaction();
    }

    /**
     * @return mixed
     */
    public function wrapInTransaction(callable $func) : callable
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

    /**
     * @param string $dql
     * @return ORM\Query
     */
    public function createQuery(string $dql = '') : ORM\Query
    {
        return $this->getWrappedEm()->createQuery($dql);
    }

    /**
     * @param string $sql
     * @return ORM\NativeQuery
     */
    public function createNativeQuery(string $sql, ResultSetMapping $rsm) : ORM\NativeQuery
    {
        return $this->getWrappedEm()->createNativeQuery($sql, $rsm);
    }

    /**
     * @return ORM\QueryBuilder
     */
    public function createQueryBuilder() : ORM\QueryBuilder
    {
        return $this->getWrappedEm()->createQueryBuilder();
    }

    /**
     * @param string $entityName
     * @param mixed  $id
     * @psalm-param class-string $entityName
     * @return object|null
     * @throws ORMException
     */
    public function getReference(string $entityName, $id) : ?object
    {
        return $this->getWrappedEm()->getReference($entityName, $id);
    }

    public function close() : void
    {
        $this->getWrappedEm()->close();
    }

    /**
     * @param object                     $entity
     * @param int                        $lockMode
     * @param int|DateTimeInterface|null $lockVersion
     * @psalm-param LockMode::* $lockMode
     * @psalm-return void
     * @throws OptimisticLockException
     * @throws PessimisticLockException
     */
    public function lock(object $entity, LockMode|int $lockMode, int|DateTimeInterface|null $lockVersion = null) : void
    {
        $this->getWrappedEm()->lock($entity, $lockMode, $lockVersion);
    }

    /**
     * @return EventManager
     */
    public function getEventManager() : EventManager
    {
        return $this->getWrappedEm()->getEventManager();
    }

    /**
     * @return ORM\Configuration
     */
    public function getConfiguration() : ORM\Configuration
    {
        return $this->getWrappedEm()->getConfiguration();
    }

    /**
     * @return bool
     */
    public function isOpen() : bool
    {
        return $this->getWrappedEm()->isOpen();
    }

    /**
     * @param string|int $hydrationMode
     * @psalm-param string|AbstractQuery::HYDRATE_* $hydrationMode
     * @return AbstractHydrator
     * @throws ORMException
     */
    public function newHydrator(int|string $hydrationMode) : AbstractHydrator
    {
        return $this->getWrappedEm()->newHydrator($hydrationMode);
    }

    /**
     * @psalm-suppress DeprecatedClass
     * @return ORM\Proxy\ProxyFactory
     */
    public function getProxyFactory() : ORM\Proxy\ProxyFactory
    {
        return $this->getWrappedEm()->getProxyFactory();
    }

    /**
     * @return ORM\Query\FilterCollection
     */
    public function getFilters() : ORM\Query\FilterCollection
    {
        return $this->getWrappedEm()->getFilters();
    }

    /**
     * @return bool
     */
    public function isFiltersStateClean() : bool
    {
        return $this->getWrappedEm()->isFiltersStateClean();
    }

    /**
     * @return bool
     */
    public function hasFilters() : bool
    {
        return $this->getWrappedEm()->hasFilters();
    }

    /**
     * @param string   $className
     * @param mixed    $id
     * @param int|null $lockMode
     * @param int|null $lockVersion
     * @psalm-param class-string $className
     * @psalm-param LockMode::*|null $lockMode
     * @return object|null
     */
    public function find(string $className, $id, LockMode|int|null $lockMode = null, ?int $lockVersion = null) : ?object
    {
        return $this->getWrappedEm()->find($className, $id, $lockMode, $lockVersion);
    }

    /**
     * @param object $object
     */
    public function persist(object $object) : void
    {
        $this->getWrappedEm()->persist($object);
    }

    /**
     * @param object $object
     */
    public function remove(object $object) : void
    {
        $this->getWrappedEm()->remove($object);
    }

    public function clear() : void
    {
        $this->getWrappedEm()->clear();
    }

    /**
     * @param object $object
     */
    public function detach(object $object) : void
    {
        $this->getWrappedEm()->detach($object);
    }

    /**
     * @param object $object
     * @param int|LockMode|null $lockMode
     */
    public function refresh(object $object, LockMode|int|null $lockMode = null) : void
    {
        $this->getWrappedEm()->refresh($object, $lockMode);
    }

    public function flush() : void
    {
        $this->getWrappedEm()->flush();
    }

    /**
     * @return ORM\Mapping\ClassMetadataFactory
     */
    public function getMetadataFactory() : ORM\Mapping\ClassMetadataFactory
    {
        return $this->getWrappedEm()->getMetadataFactory();
    }

    /**
     * @param object $obj
     */
    public function initializeObject(object $obj) : void
    {
        $this->getWrappedEm()->initializeObject($obj);
    }

    /**
     * @param object $object
     * @return bool
     */
    public function contains(object $object) : bool
    {
        return $this->getWrappedEm()->contains($object);
    }

    public function reopen() : void
    {
        $context = Co::getContext(Co::getCid());
        $em      = $this->getWrappedEm();
        if ($em->isOpen()) {
            $em->clear();
        } else {
            unset($context->entityManager);
        }
    }
}
